# ADR 0012 — Motore allarmi deterministico, Event Outbox bounded e modello di capacità

- **Stato:** **proposto per review indipendente** (VS-005). Implementato e misurato su questa branch; **non mergiato**. Allarmi **disabilitati per default** (sezione `[alarms]` assente).
- **Data:** 2026-11-01
- **Ambito:** `scada-core::{alarm (nuovo), config, kernel, types}`, `scada-runtime::{alarms (nuovo), sequencer, ingress, server, config, budget, capture, convert}`, `bin/scada-kernel`, wire `scada.alarm.v1` (**nuovo namespace**, socket separato), `scada.driver.v1` (un codice d'errore additivo, minor **1.1 invariato**), `scada.introspect.v1` (campo additivo), `scada-sim-driver` (`AlarmClient`), `scada-bench`, fuzz, `scripts/alarm_capacity.py`.
- **Base:** `main` `b762242` (merge della PR #4, VS-004).
- **Durabilità:** **nessuna.** Stati allarme ed eventi vivono solo in RAM e si perdono al riavvio o al crash. VS-005 **NON è PRODUCTION-READY FOR LONG-RUN ALARM RETENTION** (§D7).

## 1. Contesto

La RTDB è la fotografia dell'impianto; gli allarmi sono il primo prodotto delle sue transizioni verso l'esterno (il secondo saranno i command intents, VS-007). VS-005 aggiunge il motore allarmi **dentro** `Kernel::apply`, con la stessa atomicità e lo stesso ordine totale della RTDB, e un outbox di eventi limitato e **mai sovrascritto** finché un evento non è confermato durevole. Il journal (VS-006) non esiste ancora: il contratto di commit (sequenze, watermark, recupero) viene fissato e verificato adesso con un sink simulato **solo di test**, in modo che VS-006 colleghi il journal senza cambiare l'automa.

## 2. Invarianti (brief §2) e dove si verificano

| # | Invariante | Verifica |
|---|---|---|
| I1 | Purezza e ordine totale, `AlarmAcknowledge` compreso | `AlarmEngine` in `scada-core` (nessun clock/I/O/lock, audit di purezza in `check.sh`); ACK e watermark sono `Operation` sequenziate e catturate (`A`, `W`); `episode_and_ack_over_ipc_and_replay`, proptest, fuzz `alarm_transitions` (replay: stesso digest e stesso events digest) |
| I2 | Atomicità integrale | preflight completo prima di ogni mutazione (§D4); `Err/Ignored/Duplicate ⇒ snapshot, outbox, digest identici` nel proptest (384 casi, capacità 6/7/11/64) e nel fuzz a ogni passo; `t07_*` con **20 000 eventi in una sola disconnessione** tutto-o-niente; `event_sequence_exhaustion_is_atomic` |
| I3 | Nessuna perdita silenziosa | slot `N` riusabile solo se `durable_watermark ≥ N` (§D5); `OutboxCapacityExceeded{required,free}` prima di ogni mutazione; fuzz «ogni evento sopra il watermark resta leggibile e identico»; `full_outbox_refuses_whole_batch_then_recovers` |
| I4 | Bounded, zero allocazioni in `apply` | tutto preallocato in `Kernel::new` (stati, indice, ring, staging = worst fan-out); `alarm_apply_never_grows_internal_storage`; counting allocator in `scada-bench alarms-core`: **0,000 allocazioni/op** in tutti i casi misurati |
| I5 | Qualità ortogonale: non-Good = Indeterminate, mai ritorno automatico a Normal | `evaluate()` (§D3); `t03/t04`, fuzz («un Returned di una regola di processo solo con input Good»), `supervision_silence_drives_bad_quality_but_never_auto_normal` |
| I6 | ACK sicuro | token = op seq dell'ultima attivazione/retrigger; `kernel_instance_id` obbligatorio per l'ACK; `actor_uid` = uid del peer (`SO_PEERCRED`), **mai** dal client; `control_plane_security`, `restart_is_a_new_instance` |
| I7 | Default-off invariato | senza `[alarms]`: nessun motore, outbox, socket, thread; fingerprint/digest v1/v2 **byte-identici** (golden VS-004 invariati); wire driver 1.1 immutato; FIFO default; `default_off_has_no_alarm_surface`; A/B in `docs/benchmarks/vs005.md` |
| I8 | Durabilità solo da watermark futuro | `AdvanceDurableWatermark` sequenziato e monotono; in VS-005 lo emette solo il sink simulato (`--test-simulated-durable-sink`, `Durability = SIMULATED_TEST_ONLY`); nessun testo/endpoint dichiara durabilità |

## 3. Opzioni considerate

**Dove valutare le regole.**

- (a) Watcher Tokio che legge snapshot RTDB e genera eventi in ritardo. **Scartata** dal brief (§3): ordine e atomicità diverse da quelle della RTDB, eventi persi fra due snapshot.
- (b) Clonare il `Kernel` per rollback. **Scartata**: O(stato) per op, allocazioni.
- (c) **Scelta:** `AlarmEngine` puro, campo di `Kernel` (`Option<Box<AlarmEngine>>`, `None` senza regole), valutato nel **preflight** dello stesso `apply` e commitato con la RTDB.

**Outbox pieno.**

- Sovrascrivere il più vecchio (cache lossy): **vietato** (I3).
- Applicare la RTDB e scartare gli eventi: **vietato** (I2).
- **Scelto:** rifiutare **l'intera operazione** con `OutboxCapacityExceeded{required,free}` prima di qualsiasi mutazione. Le operazioni che non generano eventi passano (§D6).

**Disconnessione senza posti.** Un'operazione di link non ha un client che possa ritentare. Alternative: (a) riserva di slot nel core dedicata ai link; (b) applicare la disconnessione senza eventi; (c) **scelta:** il runtime **differisce** la disconnessione (il link resta `Connected` nel kernel, i tag mantengono la qualità) e la ritenta, nell'ordine, appena il watermark rilascia slot; nel frattempo lo stato è CRITICAL e `pending_link_transitions` è esposto. (b) violerebbe I3; (a) è un'estensione possibile (§9) ma non risolve il caso di riserva esaurita e sposta il problema.

**Indice regole.** HashMap tag→regole: **scartata** (hash e cache miss sul percorso caldo). **Scelto:** CSR `offsets[tag_index] .. offsets[tag_index+1]` in `by_tag` (indici di regola `u32`), regole ordinate per `alarm_id`; una transizione che tocca `m` tag visita solo le regole di quei tag.

**Indice `active_or_unacked`.** Lista/HashSet: **scartati**. **Scelto:** bitset a due livelli (`ListedIndex`: parole da 64 bit + bitmap delle parole non vuote); inserimento/rimozione O(1), snapshot paginato O(pagina + parole vuote saltate a 64 per volta), nessuna scansione di tutte le regole per batch o per tick.

**Hash degli eventi.** Rolling hash per evento nel digest di stato: **scartato** (costo per evento sul percorso caldo). **Scelto:** il digest v3 include gli stati e i contatori dell'outbox (`next_event_seq`, watermark, capacità); il contenuto del ring ha un **events digest on-demand** O(capacità) per test e replay, mai nel percorso caldo.

## 4. Decisioni

### D1 — Configurazione (`[alarms]`, assente = disabilitato)

```toml
[alarms]
max_rules = 16                    # tetto del rule table (default = numero di regole); 1..=4 194 304
outbox_capacity_events = 65536    # slot di EVENTI (48 B ciascuno), non byte; >= worst fan-out
outbox_high_percent = 80          # soglia HIGH della diagnostica (CRITICAL = rifiuti / free == 0)
control_socket_path = "/run/moscascada/alarm.sock"
control_authorized_uids = [1000]  # obbligatorio con il socket
max_control_connections = 4       # 1..=64
control_page_max = 1024           # 1..=65536

[[alarms.rules]]
id = 10021                        # u32 != 0, unico
name = "P204_BEARING_HIGH"        # 1..=64 byte, [A-Za-z0-9_.-], unico
tag_id = 103
kind = "high"                     # bool_equals | high | low | bad_quality
trip = 8500
reset = 8000                      # high: reset < trip; low: reset > trip
severity = "major"                # info | warning | major | critical
```

Validazione fail-fast all'avvio (`ConfigError`): `NoAlarmRules`, `TooManyAlarmRules`, `ZeroAlarmId`, `DuplicateAlarmId`, `DuplicateAlarmName`, `AlarmUnknownTag`, `AlarmTypeMismatch`, `AlarmInvalidThresholds`, `OutboxTooSmallForFanout{capacity, worst_fanout, source_id}` (la capacità deve contenere il fan-out massimo di **una** operazione: tutte le regole sui tag della sorgente più ricca), oltre ai limiti del runtime. Le regole sono ordinate per `alarm_id`: l'ordine TOML non conta (`t02`). `--check-config` stampa regole, worst fan-out, byte del core e del control plane, layout e `durability=NONE`.

### D2 — Layout (misurato da `layout_sizes()` e test)

| Struttura | Byte | Note |
|---|---:|---|
| `CompiledRule` | 24 | `trip`, `reset`, `tag: u32`, `kind: u8`, `severity` — contiguo, ordinato per id |
| `AlarmState` | 32 | `token`, `occurrence`, `last_event_seq` (u64), `flags: u8` (active, unacked, input_valid, armed) |
| `AlarmEvent` | 48 | record `Copy` senza puntatori (§D5) |
| staging (preflight) | 48 × worst fan-out | preallocato |
| CSR | 4 × (tag + 1) + 4 × regole | |
| `ListedIndex` | ≈ regole / 8 + regole / 512 | bitset a due livelli |
| ring | 48 × `outbox_capacity_events` | prefillato in `Kernel::new` |

Nomi, id e tag id stanno nel `AlarmTable` della configurazione (metadati freddi), non nella cella dinamica. Limiti: **rappresentazione** `MAX_ALARM_RULES_CEILING = 2^22` regole e `MAX_OUTBOX_CAPACITY_CEILING = 2^28` slot (indici `u32`, contatori `u64` con `checked_add`); **configurazione** `max_rules` / `outbox_capacity_events` / `memory_budget_bytes`; **sostenuti misurati**: `docs/benchmarks/vs005.md` (1 000 000 regole costruite in ≈ 0,6 s, 100 000 regole misurate sul percorso caldo).

### D3 — Automa dell'episodio e politica di qualità

`AlarmFlags` → `EpisodeState`: `Normal` (non attivo, riconosciuto), `ActiveUnacked`, `ActiveAcked`, `ReturnedUnacked`. `input_valid` è una **dimensione separata**.

Valutazione di una regola quando cambia valore o qualità del suo tag (`evaluate`):

- `BoolEquals{expected}`: condizione = `value == expected`, solo con qualità `Good`.
- `High{trip,reset}`: se già attivo resta attivo finché `value > reset`; se non attivo scatta a `value >= trip`. `Low` speculare (`value < reset` / `value <= trip`). Limiti `i64::MIN/MAX` provati.
- Qualità non `Good` (`Unknown`, `Uncertain(SourceUnavailable|SourceSilent|StaleData)`): regola di processo **Indeterminate**: `input_valid = false`, latch active/ACK **invariato**, nessun evento. Il ritorno a `Good` rivaluta valore e isteresi.
- `BadQuality`: armata dalla prima qualità `Good` dell'istanza (nessuno storm all'avvio con `Unknown`); una volta armata è attiva finché la qualità non è `Good`.

Transizioni (brief §5): trip da `Normal` → `ActiveUnacked`, `occurrence += 1` (checked), `token = op_seq`, `Raised`; clear da attivo → `Returned` (da `ActiveAcked` chiude l'episodio); ACK → `Acknowledged`; trip da `ReturnedUnacked` → **stessa occurrence**, `Retriggered`, `token = op_seq` (il vecchio token diventa stantio); trip ripetuto da attivo → nulla. Un'osservazione identica `Good` che rinfresca solo il marker VS-004 non produce valutazioni né eventi.

ACK (`Operation::AlarmAcknowledge{alarm_id, expected_activation_token, actor_uid}`): `UnknownAlarm`, `NotAcknowledgeable` (mai attivato), `StaleActivationToken{current}` (anche se l'allarme è ancora attivo), `Ignored(AlreadyAcknowledged)`, oppure `Applied` con un evento `Acknowledged` che porta `actor_uid`. L'uid è l'identità del **processo** peer, non necessariamente di una persona; il campo è predisposto per un principal autenticato futuro.

### D4 — Aggancio a `Kernel::apply` (preflight → commit)

Per ogni operazione che può cambiare valore o qualità (`ObservationBatch`, `SourceConnected`, `SourceDisconnected`, `SourceSilenceExpired`, `FreshnessExpired`):

1. **Validazione VS-001..004 invariata** (sequenza, generazione, canali, tipi, revisioni) — nessuna mutazione.
2. **Preflight allarmi** (`AlarmEngine::begin` + `stage_tag` per ogni tag il cui (valore, qualità) cambierebbe): per ogni regola del tag (CSR) calcola il nuovo stato in uno **staging preallocato**; nessuna scrittura. `check()` verifica `staged ≤ free` (`OutboxCapacityExceeded{required, free}`), `next_event_seq + staged` senza overflow (`EventSequenceExhausted`), occorrenze senza overflow (`AlarmCounterOverflow`), staging entro il worst fan-out (`AlarmFanoutExceeded`, difesa in profondità).
3. Solo se tutto è valido: commit **infallibile** di RTDB, poi `alarm_commit(op)`: ordina lo staging per indice di regola (= `alarm_id`), assegna `event_seq` consecutivi e `ordinal_in_operation`, scrive gli slot `(seq − 1) mod C`, aggiorna stati, `ListedIndex`, `last_alarm_change_operation`, high-water.

Ogni regola dipende da un solo tag e un tag compare al massimo una volta per operazione (canali duplicati rifiutati dalla validazione), quindi ogni regola è valutata al più una volta per op. Per regole multi-tag future (VS-007) servirà un *eval stamp* per regola (u64 op seq) per deduplicare: non introdotto ora per non pagarne il costo. `FreshnessExpired` usa la lista intrusiva O(eleggibili) di VS-004 sia nel preflight sia nel commit: un impianto interamente stale non ha candidati e il tick costa O(1).

### D5 — Event Outbox

`AlarmEvent { event_seq, operation_seq, occurrence_id, activation_token, alarm_id, ordinal_in_operation, actor_uid, kind, status_after }` (48 B). `kernel_instance_id` sta nell'envelope della risposta, non nel record. Eventi di una stessa operazione: contigui, ordinati per `alarm_id`, `ordinal_in_operation` da 0. Nessun timestamp (VS-006 aggiungerà un'annotazione temporale separata dall'ordine logico).

Contatori: `next_event_seq` (da 1, mai wrap), `durable_watermark` (prefisso continuo confermato), `used = next_event_seq − 1 − durable_watermark`, `free = C − used`, `high_water`, `oldest_visible = max(1, newest − C + 1)`. **Regola irrevocabile:** lo slot dell'evento `N` è riusato solo dall'evento `N + C`, ammesso solo se `durable_watermark ≥ N`. Gli eventi rilasciati restano leggibili finché il loro slot non viene riusato. Letture (`read`, snapshot, cursori HMI) non spostano mai il watermark.

`Operation::AdvanceDurableWatermark{up_to_event_seq}`: sequenziata, catturata (`W`), monotona; `WatermarkBeyondCommitted`, `WatermarkRegression`, `Ignored(WatermarkUnchanged)`. Envelope futuro per VS-007: `EffectRecord{event_seq, operation_seq, ordinal, payload: AlarmEvent | CommandIntent}` — stessa sequenza totale; non implementato.

### D6 — Ammissione e backpressure (runtime)

| Origine | Outbox insufficiente | Effetto |
|---|---|---|
| Batch driver | **ACK `REJECTED` con `ERROR_CODE_OUTBOX_CAPACITY_EXCEEDED = 44`** (nessun `ProtocolError`, la sessione resta aperta) | nulla applicato, `source_sequence` non consumata: il driver ritenta **la stessa sequenza** più tardi (stesso contratto di `QueueFull`) |
| Hello (connect/takeover) | `HandshakeRejected` con codice 44 | il driver si ricollega più tardi |
| Disconnessione | differita (§3), ritentata in ordine dopo ogni rilascio | link `Connected` nel kernel, `pending_link_transitions` > 0, CRITICAL |
| Watchdog VS-004 | operazione non applicata, ritentata al tick successivo | nessuna scadenza inventata |
| ACK operatore | `ACK_STATUS_OUTBOX_CAPACITY_EXCEEDED` con `required/free` | episodio invariato |

Operazioni a zero eventi (valori nella fascia d'isteresi, regole non toccate, refresh) sono **applicate** anche con outbox pieno: la semantica degli allarmi è valutata sui valori reali, solo le *transizioni* richiedono slot. Stato di overload (`Normal` / `High` ≥ `outbox_high_percent` / `Critical` se `free == 0`, un rifiuto dopo l'ultimo rilascio o disconnessioni pendenti) esposto in `CapacityStatus` e `AlarmSummary` e segnalato una volta su stderr a ogni cambio: **mai** come evento nel ring pieno. Il sequencer non si blocca mai su un sink.

Contratto end-to-end: un input rifiutato **non è applicato** e può andare perso **fuori dal kernel** se il driver/dispositivo non lo conserva. Con l'outbox pieno e senza journal (VS-005) nessun nuovo allarme può essere registrato finché il processo non riparte.

### D7 — Limitazione deliberata di VS-005: NON PRODUCTION-READY FOR LONG-RUN ALARM RETENTION

Senza journal nessun watermark avanza in produzione: la capacità finisce dopo `C` eventi, poi ogni transizione d'allarme è rifiutata (CRITICAL). `time_to_full = C / λ` (`scripts/alarm_capacity.py`, μ = 0). Lo stato e gli eventi si perdono al riavvio (nuovo `kernel_instance_id`; cursori, token ed episodi del vecchio istanza rifiutati con `INSTANCE_MISMATCH`). **VS-006 è prioritaria subito dopo VS-005.**

### D8 — Control plane `scada.alarm.v1`

Socket UDS **separato** (`control_socket_path`, modo 0660, directory privata come gli altri socket), allowlist `control_authorized_uids` su `SO_PEERCRED`, `max_control_connections` (eccedenti: `TOO_MANY_CONNECTIONS` e chiusura), richiesta ≤ 1024 B (`FRAME_TOO_LARGE`), timeout di frame e idle 60 s, `alarm_api_version = 1`, `kernel_instance_id` (vuoto = qualsiasi per le letture, **obbligatorio per l'ACK**; diverso ⇒ `INSTANCE_MISMATCH`). Tutte le richieste attraversano il sequencer (corsia di controllo dell'ingresso): ogni risposta riflette uno stato a un `operation_sequence` preciso.

- `GetAlarmSnapshot{after_alarm_id, limit, expected_change_operation}`: pagina degli allarmi `active || unacked` in ordine di id; `change_operation` = ultima operazione che ha cambiato lo stato degli allarmi; se cambia fra due pagine ⇒ `SNAPSHOT_CHANGED` (ricominciare): mai uno snapshot «misto».
- `ReadEvents{after_event_seq, limit}`: pagina, `next_cursor`, `oldest_visible`, `newest`, `has_more`, `gap_detected` (cursore dietro agli slot riusati), `cursor_ahead`, `durable_watermark`. Non sposta il watermark.
- `AcknowledgeAlarm{alarm_id, activation_token}` → `AckStatus`; l'actor è il peer.
- `GetCapacityStatus`: capacità, used/free, high water, sequenze, **durability** (`NONE_VOLATILE` o `SIMULATED_TEST_ONLY`), overload, contatori di rifiuto per origine, regole, worst fan-out, dimensioni dei record, memory bound.
- `SimulatedDurableCommit{up_to}`: **solo** con `--test-simulated-durable-sink`; altrimenti `UNSUPPORTED`.

Pagine limitate da `control_page_max`, buffer per sessione riciclati; costo O(pagina), mai O(eventi nel ring).

### D9 — Digest, fingerprint, capture

- Nessuna regola: fingerprint e digest **v1/v2 byte-identici** a VS-004.
- Con regole: **fingerprint v3** = corpo v2 + flag di supervisione + rule table canonica (id, nome, tag, kind, trip, reset, severità, `max_rules`); la **capacità dell'outbox non è nel fingerprint** (non cambia il significato delle regole) ma è nel **digest di stato v3** = corpo v2 + stati di ogni regola + `last_alarm_change_operation` + capacità + `next_event_seq` + watermark. Events digest on-demand per il contenuto del ring.
- Capture: `A <op> <alarm_id> <token> <uid>` e `W <op> <up_to>`; gli eventi non si catturano (derivati deterministicamente). Replay: stessa RTDB, stati, eventi, token, digest ed events digest.

### D10 — Memoria e budget

`Kernel::alarm_storage_bytes` (stati + indice + ring + staging + finestre quiet per tag + `staged_tags`) è dentro il termine `kernel` di `budget.rs`; il runtime aggiunge `alarms` (control plane: sessioni × pagine riusabili + richieste + stima per task; lista delle disconnessioni pendenti). `memory_budget_bytes` rifiuta l'avvio se superato. Il profilo Micro rappresenta **4 GiB fisici di macchina**, non del processo: il kernel resta nell'ordine dei MiB senza allarmi; con allarmi il ring domina (48 B/slot: 1 Mi slot = 48 MiB). Gli esempi di dimensionamento sono in `docs/benchmarks/vs005.md` §capacità.

### D11 — Finestra «quiet» per tag (ottimizzazione del preflight, review PR #5 P1)

**Problema misurato.** Con allarmi attivi, la versione iniziale di questa PR spendeva circa 10 ns per regola e circa 1,3 µs per un batch da 128 tag anche quando nessuna regola cambiava, con un secondo passaggio sui tag staged. Con `multi_thread(2)` il sequencer è il collo di bottiglia e il throughput calava del 16…26 %.

**Decisione.** L'`AlarmEngine` mantiene per ogni tag una finestra `[lo, hi]` di valori (bool come 0/1). Un input `Good` con valore nella finestra non cambia **nessuna** regola del tag. È l'intersezione delle finestre delle sue regole:

| tipo di regola | condizione | finestra |
|---|---|---|
| `high` | non attiva | `[MIN, trip−1]` |
| `high` | attiva | `[reset+1, MAX]` |
| `low` | non attiva | `[trip+1, MAX]` |
| `low` | attiva | `[MIN, reset−1]` |
| `bool_equals` | — | il solo valore che mantiene lo stato |
| `bad_quality` | armata e non attiva | tutto |
| `bad_quality` | non armata o attiva | vuota |
| qualsiasi | input non ancora valido | vuota |
| tag senza regole | — | tutto |

- **Uso nel preflight.** Il preflight dei batch, ora un solo passaggio fuso con il controllo di overflow delle revisioni, legge la finestra del tag e salta CSR, regole e stati se il valore vi cade. Altrimenti valuta le regole come prima.
- **Aggiornamento.** Il commit ricalcola la finestra solo dei tag con regole staged, una volta per tag (`staged_tags`, capacità pari al fan-out peggiore, nessuna crescita). Le transizioni di qualità (disconnessione, silenzio, freschezza) non usano la finestra.
- **Esattezza, non euristica.** Il test `quiet_window_is_exact` confronta la finestra con `evaluate` su ogni tipo di regola, ogni combinazione di flag e i valori estremi e di soglia. Nelle build di debug, quindi nei test e nel fuzz, ogni salto è verificato con `assert_quiet_exact`. La finestra è stato derivato: non entra in digest, fingerprint o capture, e il replay la ricostruisce.
- **Costo.** 16 B per tag più 4 B × fan-out peggiore, inclusi in `alarm_storage_bytes` e nel budget. Le operazioni con transizioni pagano il ricalcolo, circa il +3…12 % p50 in-process. Misure in `docs/benchmarks/vs005.md` §2.3.
- **Invariato:** preflight atomico prima di qualsiasi mutazione, zero allocazioni, determinismo, bound e precedenza degli errori. Un overflow di revisione vince su un errore di staging allarmi; l'ammissione nell'outbox viene per ultima.

### D12 — Semantica temporale degli eventi (vincolante per VS-006; nessun codice in VS-005)

In VS-005 né eventi né capture portano tempo. Prima del journal fissiamo cosa significherà, perché con un backlog la scrittura durevole può avvenire molto dopo il trip.

1. **L'ordine non dipende mai dal tempo.** Ordine totale = `operation_seq`, poi `event_seq`, come oggi. Un orologio che salta non riordina nulla.
2. **Tre istanti distinti, mai confusi:**
   - **`sequenced_at`, tempo dell'evento.** Il thread del sequencer lo cattura (CLOCK_REALTIME e CLOCK_MONOTONIC) subito prima di `Kernel::apply` dell'operazione a cui assegna `operation_seq`: è l'istante in cui il sistema riconosce la transizione. Ce n'è uno per operazione e tutti gli eventi dell'operazione lo condividono. È il tempo mostrato all'operatore.
   - **`received_at`, tempo di ingresso.** Facoltativo: ricezione del frame dal driver. Misura la coda d'ingresso e non è il tempo dell'evento.
   - **`durable_at`, tempo di persistenza.** Il journal lo registra quando l'fsync che copre l'evento si conclude. `durable_at − sequenced_at` è il ritardo di persistenza, esposto come metrica. Non sostituisce mai il tempo dell'evento.
3. **Il core resta senza orologi.** `sequenced_at` è un'annotazione del runtime indicizzata per `operation_seq`. Viene scritta nel record di operazione del journal e nella capture come dato d'ingresso, ignorata da `apply`. Il record caldo `AlarmEvent` resta di 48 B. Il replay legge i tempi registrati e non li ricalcola: il replay resta byte-identico.
4. **Salti dell'orologio.** Il realtime può tornare indietro (NTP, RTC). Il runtime registra anche il monotonic e segnala `clock_regressed` quando il realtime di un'operazione è minore di quello della precedente. Non corregge e non riordina.
5. **Tempo di sorgente.** Il timestamp di campo del driver non è nel protocollo 1.1. Se verrà aggiunto, sarà per osservazione, dichiarato non fidato e distinto da `sequenced_at`.

## 5. Casi limite

- 20 000 eventi da una sola disconnessione: tutti o nessuno (`t07`).
- Outbox esattamente pieno (free == required) ⇒ accettato; C+1 ⇒ rifiutato, stato identico (misurato in `alarms-core` D).
- Watermark oltre il committed, regressione, invariato: rifiutati/ignorati senza consumare op seq.
- Retrigger dopo ritorno non riconosciuto: stessa occurrence, nuovo token; ACK col vecchio token rifiutato.
- Takeover di un link durante una disconnessione differita: la disconnessione differita porta la vecchia generazione ⇒ `Ignored(StaleLinkGeneration)` al retry.
- Riavvio: nuovo istanza, nessuna falsa ripresa (`restart_is_a_new_instance`).

## 6. Costi (misurati, dettagli in `docs/benchmarks/vs005.md`)

Vedi `docs/benchmarks/vs005.md` per i valori assoluti e la varianza: A/B VS-004 vs VS-005 disabilitato, overhead con regole senza trip, storm, fan-out, rifiuto a outbox pieno, storm di processo 60 s con sink attivo e fermo, soak.

Sintesi (VM 4 vCPU, client co-residenti; valori assoluti solo relativi):

- **Default-off:** nessuna regressione ripetibile di throughput in 25 celle (ct, mt2, `source_fair`, supervisione); RSS +2,5…+3,7 % (codice in più nel binario).
- **Allarmi on, nessun trip (2 057 regole), codice finale (D11):**
  - `multi_thread(2)` pipelined: −1,2 % / −0,2 % di batches/s contro main, non significativo (conferma n = 10).
  - `current_thread`: throughput invariato, CPU +0,8…+2,8 %.
  - RSS: ring preallocato (48 B/slot) più 16 B per tag.
  - La versione iniziale della PR, senza finestra quiet, perdeva il −16…−26 % con mt2 pipelined: il sequencer pagava circa 10 ns per regola valutata, circa +1,3 µs per un batch da 128. La review (PR #5, P1) l'ha respinta come compromesso e il costo è stato rimosso (`vs005.md` §2.2–§2.3).
- **Transizioni:** il ricalcolo della finestra costa circa +3…+12 % p50 in-process.
- **In-process, codice finale:**
  - zero allocazioni in `apply` in ogni caso;
  - 100 000 regole a riposo: p50 3,25 µs, pari a «0 regole»;
  - storm da 1 000 eventi/op: 25 M eventi/s;
  - fan-out 8 192: ≈ 0,34 ms p50;
  - ACK: 0,1 µs;
  - 1 000 000 di regole costruite in ≈ 0,5 s, 151 B/regola.
- **Profilo Micro:** `config/profiles/micro-alarms.toml` (budget 64 MiB, 524 288 slot = 24 MiB): ≈ 14,5 h di ritenzione a 10 eventi/s senza journal.

## 7. Compatibilità

Additivo: nuovo namespace protobuf, un codice d'errore driver nuovo (44, solo con allarmi configurati), campo introspezione `alarms = 11`, nuove varianti di `Operation`/`ApplyError`/`IgnoredReason` nel core (API Rust interna). Driver 1.0/1.1 invariati; un driver legacy che riceve il codice 44 lo tratta come qualsiasi `REJECTED` con codice sconosciuto (ritenta o si ferma, secondo la sua policy).

## 8. Garanzie per slice

| | VS-005 (questa) | VS-006 (prossima) | VS-007 |
|---|---|---|---|
| Valutazione | deterministica, atomica con la RTDB, incrementale, zero allocazioni | invariata | regole multi-tag, logiche |
| Eventi | ordine totale, in RAM, **nessuna sovrascrittura di eventi non durevoli**, backpressure tipizzata | journal durevole (commit/fsync secondo policy), watermark reale | `EffectRecord` con `CommandIntent` nella stessa sequenza |
| ACK | sequenziato, token anti-stale, uid peer, **volatile** | ACK e note operatore durevoli, principal autenticato | — |
| Riavvio | tutto perso, nuovo instance id | recupero da journal, Active Alarm Projection | riconciliazione di esecuzione |
| Capacità | finita: `C` eventi per vita del processo (μ = 0) | μ = rate del journal; `alarm_capacity.py` già pronto | — |

## 9. Conseguenze, rischi, estensioni

- **Rischio principale:** in produzione senza journal l'outbox si riempie e gli allarmi successivi sono rifiutati. Mitigazione: dimensionamento esplicito, stato CRITICAL osservabile, VS-006 subito dopo.
- Disconnessione differita: il link appare `Connected` più a lungo del reale mentre l'outbox è pieno (dichiarato in `pending_link_transitions`). Estensione possibile: riserva di slot per transizioni di link (`--reserve` del modello).
- **Costo per tag toccato.** Ora è un solo confronto con la finestra quiet del tag (D11), anche per i tag senza regole. Il CSR si percorre solo quando una regola può cambiare. Misurato in `alarms-core` A e in `alarms-hot`.
- **Stato derivato da tenere coerente.** La finestra quiet deve essere ricalcolata a ogni commit che cambia uno stato di regola. Qualsiasi futuro percorso che modifichi `states` fuori da `commit`, per esempio un ripristino da journal in VS-006, deve ricostruirla. È verificata in debug (`assert_quiet_exact`) e dal fuzz.
- **Timestamp:** semantica fissata in D12 prima del journal.
- Catalogo operatore fuori dal core: schema in `docs/alarm-catalog.md`.
