# ADR 0011 — Liveness delle sorgenti, heartbeat e freschezza dei tag

- **Stato:** **proposto per review** (VS-004). Implementato e misurato su questa branch; nessun default cambiato. Supervisione **disabilitata per default**.
- **Data:** 2026-11-01
- **Ambito:** `scada-core::{types, config, kernel}` (operazioni e qualità nuove), `scada-runtime::{supervision (nuovo), sequencer, ingress, server, config, budget, capture, convert, diag}`, wire `scada.driver.v1` **minor 1.1** (additivo), `scada.introspect.v1` (additivo), `scada-sim-driver`, `scada-bench`, fuzz.
- **Base:** `main` `1071547` (merge della PR #3, VS-003).

## 1. Contesto

Fino a VS-003 la sola degradazione è `SourceDisconnected` → `Uncertain(SourceUnavailable)`. Due guasti restano invisibili:

1. **Socket aperto, driver fermo** (processo bloccato, dispositivo muto, thread di acquisizione morto): il link resta `Connected` e i tag restano `Good` per sempre.
2. **Dati parziali**: un driver che continua a inviare `Delta` su alcuni canali lascia gli altri tag `Good` con valori vecchi a piacere.

VS-004 distingue tre concetti indipendenti:

| Concetto | Valori | Chi lo decide | Nel digest |
|---|---|---|---|
| Stato del link | `NeverConnected` / `Connected{gen}` / `Disconnected{gen}` | `SourceConnected` / `SourceDisconnected` (VS-001/002, invariato) | sì (v1) |
| Liveness della sorgente | `Alive` / `Silent` (introspezione: anche `NotSupervised`, `NoLink`) | `SourceSilenceExpired` / `SourceLivenessRestored` / batch applicato | sì (v2) |
| Freschezza del tag | `Good` → `Uncertain(StaleData)` | `FreshnessExpired` / osservazione applicata | sì (v2) |

**Semantica del tempo.** Il kernel non conosce l'istante di acquisizione nel dispositivo: il protocollo non trasporta timestamp affidabili e autenticati. In VS-004 **freschezza = tempo monotono trascorso dall'ultima osservazione nuova accettata dal kernel per quel tag**, misurato dal runtime con la granularità del watchdog. Non è l'età fisica certificata della misura: un driver che ritrasmette per ore lo stesso valore vecchio come osservazione nuova lo rende «fresco». Il valore `Heartbeat.sequence` non è un tempo.

## 2. Invarianti (brief §2) e dove si verificano

| # | Invariante | Verifica |
|---|---|---|
| S1 | `Kernel::apply` puro, sincrono, single writer; nessun clock nel core | audit di purezza in `scripts/check.sh` (grep `Instant::`, `SystemTime`, thread, I/O); il tempo è solo nel runtime (`supervision.rs`) |
| S2 | Qualità, liveness e digest cambiano solo tramite operazioni sequenziate | tre nuove `Operation`; cattura `S`/`R`/`X`; `capture_replay_reproduces_supervised_state`; fuzz `supervision_transitions` (replay) |
| S3 | Stato e RAM limitati | marker di dimensione fissa per tag/sorgente; anello di campioni preallocato e limitato (`MAX_FRESHNESS_POLLS`); heartbeat = contatore atomico per link; `supervised_apply_never_grows_internal_storage`; `budget.rs` |
| S4 | `operation_sequence` solo su `Applied`; no-op e scadenze obsolete non consumano nulla | `Ignored(StaleLinkGeneration / AlreadySilent / NotSilent / NothingExpired)`; proptest e fuzz verificano «no-op ⇒ snapshot e digest invariati» |
| S5 | Default-off = contratto VS-003, stesse prestazioni | nessuna sorgente sorvegliata ⇒ nessun thread, nessun tick, nessun marker mantenuto, fingerprint e digest **v1 byte-identici**; `default_off_is_the_vs003_contract`, golden digest v1, A/B in `docs/benchmarks/vs004.md` |
| S6 | `current_thread`, `multi_thread(2)`, `source_fair` | `tests/vs004.rs` in tutte le matrici di `check.sh`; suite VS-001..003 ripetuta con supervisione attiva |

## 3. Opzioni considerate

**Tempo nel core.**

- (a) `Kernel::apply(op, now)`: il core confronta istanti. **Scartata**: il replay dipenderebbe da come si ricostruisce `now`, e ogni `apply` dovrebbe ricevere un tempo anche quando non serve.
- (b) `TimerFired{deadline}` generico: il core terrebbe le scadenze. **Scartata**: richiede tempo nel core e una struttura di timer nel digest.
- (c) **Scelta:** il runtime decide *quando*; il core riceve operazioni che dicono *cosa* (`SourceSilenceExpired`, `SourceLivenessRestored`, `FreshnessExpired`) con guardie che il core può verificare senza tempo (generazione del link, token di freschezza). Il replay riapplica le stesse operazioni senza clock.

**Guardia anti-timer-stale per la freschezza.**

- revisione del tag: **insufficiente** (brief §5.2), un'osservazione identica non cambia revisione;
- istante dell'ultima osservazione nel core: **vietato** (tempo nel core);
- **scelto:** marker `observed` = `operation_sequence` del batch che ha osservato il tag per ultimo. È monotono, sta nel digest v2, ed è confrontabile con un token anch'esso espresso in operation sequence. Un'osservazione dopo il calcolo della scadenza ha un marker maggiore del token e quindi non scade.

**Timer.** Un task/timer per tag o per sorgente (N timer, N wake-up): **scartato** (brief §5.3). Heap di scadenze: inutile, perché le scadenze di una sorgente hanno tutte lo stesso timeout e si riducono a «il più grande op seq abbastanza vecchio». **Scelto:** un unico thread watchdog a cadenza fissa + scansione delle sole sorgenti sorvegliate al tick (O(sorgenti sorvegliate) per tick, O(canali) solo quando c'è qualcosa da far scadere).

**Heartbeat nella coda del sequencer.** Ogni heartbeat come richiesta: **scartato** (un flood consumerebbe coda e CPU del writer e affamerebbe i batch). **Scelto:** contatore `AtomicU64` per link, incrementato dalla sessione; il watchdog lo legge al tick. Coalescenza naturale: al più una prova di vita per tick, qualunque sia il rate.

## 4. Decisioni

### D1 — Configurazione (API confermata)

```toml
[runtime]
watchdog_poll_ms = 250          # 10..=60000; ammesso solo se almeno una sorgente è sorvegliata

[[sources]]
id = 1001
liveness_timeout_ms = 5000      # opzionale; assente = nessuna supervisione di liveness
freshness_timeout_ms = 12000    # opzionale; assente = nessuna scadenza dei tag
```

Validazione all'avvio (`LoadError::Runtime`, messaggio esplicito, exit ≠ 0): ogni timeout `>= 3 × watchdog_poll_ms` e `<= 86 400 000`; `freshness_timeout_ms / watchdog_poll_ms <= 4096` (bound dell'anello); `watchdog_poll_ms` senza alcuna sorgente sorvegliata è un errore (configurazione incoerente). Timeout di freschezza **omogeneo per sorgente**, scadenza **valutata per ciascun tag**. Liveness sola, freschezza sola, entrambe, nessuna: tutte ammesse e testate. I profili Micro/Server **non cambiano**: le chiavi vi compaiono solo come commento.

### D2 — Protocollo 1.1 e negoziazione del minor

- `Hello.protocol_minor = 5` (nuovo, additivo): il minor massimo che il driver parla per la major scelta. Un driver VS-003 non lo invia (proto3: 0).
- Il kernel risponde `Welcome.negotiated_protocol_minor = min(1, hello.protocol_minor)`.
- `DriverMessage.body` guadagna `Heartbeat heartbeat = 3 { uint64 sequence = 1 }`; nessun campo rinumerato; `ERROR_CODE_INVALID_HEARTBEAT = 15`.
- **Regola del driver:** invia heartbeat solo se `negotiated_protocol_minor >= 1`. Un kernel VS-001..003 risponde sempre minor 0 (ignora il campo 5 sconosciuto), quindi un driver aggiornato **non invia mai heartbeat a un kernel vecchio** (provato con binari reali di `main@1071547`, §8).
- Un kernel VS-004 con driver legacy negozia 1.0: tutto come prima. Se la sorgente ha `liveness_timeout_ms`, un driver legacy che non manda batch entro il timeout diventa `Silent` (atteso e documentato: senza heartbeat solo i batch provano la vita).
- Un vecchio kernel che ricevesse un `Heartbeat` lo tratterebbe come messaggio vuoto/sconosciuto (`UnexpectedMessage`, link chiuso): per questo la regola sopra è obbligatoria e non si fa affidamento sul fatto che il `oneof` nuovo sia ignorato.
- `buf breaking` contro la baseline v1.0: verde (`scripts/check-proto.sh`).

### D3 — Heartbeat: validità, nessun ACK, flood

- Nessun `InputAck` per heartbeat; nessun effetto su `source_sequence`, deduplicazione, ACK dei batch.
- La sorgente è quella del `Hello` del link; l'heartbeat non porta identità.
- Heartbeat su link negoziato 1.0 → `UnexpectedMessage`, fatale. `sequence == 0` o non strettamente crescente sul link → `INVALID_HEARTBEAT`, fatale. Entrambi contati in `heartbeats_invalid`.
- Heartbeat valido: `heartbeats_valid += 1` e, se la sorgente ha liveness, `+1` sul contatore atomico del link. **Nessuna richiesta al sequencer, nessuna allocazione, nessun op seq.**
- Flood: il costo è la decodifica del frame nella sessione (limitata da `max_frame_bytes` e dal socket); il watchdog vede al più una prova per tick (`heartbeats_coalesced` conta l'eccesso). Il recupero da `Silent` resta possibile: basta un heartbeat. Test: `heartbeat_flood_is_bounded_and_does_not_starve` (200 000 heartbeat: op seq invariato salvo transizioni legittime, RSS limitata, un'altra sorgente servita).
- Con supervisione disattivata un kernel VS-004 accetta e conta gli heartbeat di un driver 1.1 senza alcun effetto sullo stato.

### D4 — Liveness

- Inizia al `SourceConnected` del link corrente (completamento del `Welcome`): l'istante del collegamento è la prima prova.
- **Prova di vita** = heartbeat valido del link corrente **oppure** batch **applicato** della sorgente (il `last_batch_operation` del kernel cambia). Batch rifiutati, malformati, di generazioni vecchie e `Duplicate` **non** contano. *Scelta motivata:* un duplicato dimostra che il trasporto è vivo, ma è indistinguibile da un driver bloccato in un ciclo di ritrasmissione; un driver sano che vuole solo dire «sono vivo» usa l'heartbeat. Così una sola regola semplice («evidenza = heartbeat o batch nuovo») vale per liveness e freschezza.
- Nessuna prova per `>= liveness_timeout` → `SourceSilenceExpired{source, gen}`: la sorgente diventa `Silent` (logico, nel digest), il link resta `Connected` (non è un EOF e non genera `SourceDisconnected`), i tag con valore `Good` o `Uncertain` di rango inferiore passano a `Uncertain(SourceSilent)` con valore preservato. Una seconda scadenza mentre è già silente → `Ignored(AlreadySilent)`: nessuna revisione doppia, nessun op seq.
- Heartbeat su link silente → `SourceLivenessRestored{source, gen}`: torna `Alive`, **nessun tag cambia**. Solo una nuova osservazione applicata riporta `Good` il proprio tag. Un batch applicato su sorgente silente la rende `Alive` nello stesso `apply` (implicito, senza operazione separata) e porta `Good` solo i tag osservati; gli altri restano `Uncertain(SourceSilent)` finché non sono osservati.
- Socket chiuso, takeover, driver terminato: semantica VS-002 invariata (`SourceUnavailable`). Connect e disconnect azzerano `silent`. Ogni operazione porta la generazione: quella di un link sostituito → `Ignored(StaleLinkGeneration)`; il watchdog riparte da zero sul link nuovo (`on_connected`) e scarta il contatore del vecchio.

### D5 — Freschezza per tag

- Un batch applicato imposta `observed = op` per **ogni tag che contiene** (anche con valore identico, quindi anche senza nuova revisione). `FullState` = tutti i canali; `Delta` = solo quelli inclusi.
- `Duplicate`, `REJECTED`, malformati, heartbeat: nessun rinnovo. Un tag mai osservato (`observed = 0`) resta `Unknown` e non scade mai.
- Il runtime emette `FreshnessExpired{source, gen, observed_up_to = U}`; il core porta a `Uncertain(StaleData)` **solo** i tag della sorgente che sono `Good` con `0 < observed <= U`. Revisione +1 solo su quel primo cambio; un tag già `Uncertain` non cambia. Se nessun tag scadrebbe il sequencer non la invia (e il core risponderebbe `Ignored(NothingExpired)`).
- Token valido: `1 <= U < op`, cioè un'operazione già applicata; altrimenti `Err(InvalidFreshnessToken)` (atomico, nulla cambia).
- Come si calcola `U`: a ogni tick il supervisor registra `(istante del tick, last_batch_operation della sorgente)` in un anello preallocato di `ceil(T/poll) + 2` campioni. `U` = `last_batch_operation` del campione più recente con età `>= T`. Ogni tag con marker `<= U` è stato osservato per l'ultima volta prima di quel tick, quindi da almeno `T`: **mai una scadenza anticipata**. Un tag osservato dopo ha marker `> U` e non è toccato (anti-stale, anche se la scadenza è già «in volo»).
- **Costo indipendente dal numero di tag stale (review P1).** Ogni sorgente con freschezza tiene una lista intrusiva (due `u32` per cella, testa/coda per sorgente, preallocate) dei suoi tag `Good` **in ordine di marker**. Un'osservazione applicata sposta il tag in coda (O(1): il suo marker è il più grande); una degradazione della sorgente (`SourceSilent`, `SourceUnavailable`) svuota la lista (nessun tag resta `Good`); una scadenza stacca il prefisso con marker `<= U`. I tag che scadono sono quindi esattamente un prefisso della lista: `FreshnessExpired` e la query `freshness_candidates` visitano solo quei tag e costano **O(1) quando nulla è eleggibile**, per quanti tag siano già stale. Prima della review la query scorreva ogni canale della sorgente a ogni tick una volta passato il primo timeout: O(tag) per tick per sempre su un impianto tutto stale. La lista è stato derivato (funzione di qualità e marker, già in snapshot e digest), quindi digest e replay non cambiano; il proptest e il fuzz la confrontano a ogni passo con una scansione completa.

### D6 — Precedenza e transizioni

Rango: `SourceUnavailable` (3) > `SourceSilent` (2) > `StaleData` (1). Una degradazione si applica solo se il rango nuovo è **strettamente maggiore** dell'attuale; `Good` è raggiungibile **solo** da un'osservazione applicata.

| Stato del tag \ evento | batch che lo osserva | batch che non lo osserva | `FreshnessExpired` (marker ≤ U) | `SourceSilenceExpired` | `SourceLivenessRestored` / heartbeat | `SourceDisconnected` / nuovo `SourceConnected` |
|---|---|---|---|---|---|---|
| `Unknown` | `Good` | `Unknown` | `Unknown` | `Unknown` | `Unknown` | `Unknown` |
| `Good` | `Good` (rev +1 solo se cambia il valore) | `Good` | `Uncertain(StaleData)` | `Uncertain(SourceSilent)` | `Good` | `Uncertain(SourceUnavailable)` |
| `Uncertain(StaleData)` | `Good` | invariato | invariato | `Uncertain(SourceSilent)` | invariato | `Uncertain(SourceUnavailable)` |
| `Uncertain(SourceSilent)` | `Good` | invariato | invariato | invariato (`AlreadySilent`) | invariato | `Uncertain(SourceUnavailable)` |
| `Uncertain(SourceUnavailable)` | `Good` | invariato | invariato | invariato | invariato | invariato |

### D7 — Ordine fra eventi e tick (anche con `source_fair`)

- Un unico thread `scada-watchdog` alza un tick ogni `watchdog_poll_ms` (cadenza fissa; dopo uno stallo salta i tick persi invece di recuperarli a raffica). Imposta un flag atomico e, se non ce n'è già una in coda, prova (`try_send`, mai bloccante) a mettere una richiesta `Tick` nella corsia di controllo dell'ingresso (+1 slot di capacità), solo per svegliare un sequencer inattivo. Coda piena ⇒ non serve: il sequencer è occupato e vede il flag dopo la richiesta corrente.
- Il sequencer controlla il flag **dopo ogni richiesta** (una load relaxed). Al tick legge il clock **una volta** e, per le sorgenti sorvegliate in ordine di configurazione, decide e applica le operazioni con lo stesso `apply` dei batch. Un flood ritarda un tick al massimo di un `apply`, non lo affama.
- Le decisioni si prendono sullo **stato del kernel al momento del tick**, che è già il risultato di tutti i batch applicati prima. Un batch che entra dopo il tick viene applicato dopo le operazioni del tick, nello stesso ordine totale di `operation_sequence`. Con `source_fair` cambia solo l'ordine con cui i batch raggiungono `apply`, non la regola: la decisione non usa mai richieste ancora in coda.
- **Stampa dell'evidenza:** heartbeat e batch vengono datati con l'istante del tick che li osserva (limite superiore del loro istante reale). Conseguenza: una scadenza può arrivare in ritardo, mai in anticipo.

### D8 — Digest e fingerprint versionati

- Nessuna sorgente sorvegliata: digest **v1** e fingerprint **v1** byte-identici a VS-003 (test golden invariato). I marker non sono mantenuti.
- Almeno una sorgente sorvegliata: digest **v2** (`STATE_DIGEST_VERSION_SUPERVISED`) = codifica v1 + flag di supervisione, `silent` e `last_batch_operation` per sorgente, `observed` per tag; fingerprint **v2** = v1 + flag per sorgente (due configurazioni che differiscono solo per la supervisione hanno fingerprint diversi). Codici delle ragioni: `SourceUnavailable = 1` come in v1, `SourceSilent = 2`, `StaleData = 3`. `IntrospectResponse.state_digest_version` dice quale versione è in uso.
- Tutto ciò che influenza transizioni future (`silent`, `last_batch_operation`, `observed`) è nello snapshot logico e nel digest; lo stato del watchdog (istanti, anello) **non** lo è, perché non influenza `apply`: le sue decisioni sono già scritte nello stream come operazioni.

### D9 — Introspezione e metriche (additive)

`Quality`: `UNCERTAIN_SOURCE_SILENT = 4`, `UNCERTAIN_STALE_DATA = 5`. `TagState.last_observed_operation`. `SourceState.liveness` (`NOT_SUPERVISED`, `NO_LINK`, `ALIVE`, `SILENT`), `last_batch_operation`, `freshness_supervised`. Contatori operativi (non nel digest, cardinalità fissa): `heartbeats_valid/invalid/coalesced`, `liveness_expiries`, `liveness_recoveries` (ogni `Silent → Alive` dello stesso link, per heartbeat **o** per batch applicato; review P2) con la parte dovuta ai batch in `liveness_recoveries_by_batch` (il ripristino per batch avviene dentro l'`apply` del batch ed è contato dal tick successivo; disconnessione e takeover non sono recuperi), `freshness_expiries`, `freshness_tags_expired`, `watchdog_ticks`, `watchdog_ticks_coalesced`, `watchdog_ticks_late`, `watchdog_max_late_ns`; stadio diagnostico `watchdog_lateness` (ritardo fra tick alzato e tick servito). `--check-config` stampa sorgenti sorvegliate, soglie e bound di memoria; la riga `READY` riporta `supervision=off|N`. L'introspezione non modifica lo stato.

### D10 — Memoria

Default-off: 0 byte (nessun `Supervisor`, nessun thread). Le celle del core hanno sempre `observed` e i due link della lista di freschezza (16 B/tag in più rispetto a VS-003) e ogni sorgente `silent`, `last_batch_operation`, testa e coda della lista. Attiva: tabella per sorgente + anello `(Duration, u64)` di `ceil(T/poll)+2` campioni per sorgente con freschezza (max 4098 × 24 B) + 32 B per connessione (contatore heartbeat) + costante. Marker e link sono mantenuti solo se la sorgente è sorvegliata. Micro (2 MiB) e Server (16 MiB) passano con la configurazione di default e con le chiavi VS-004 attivate (`shipped_profiles_fit_their_budget_with_vs004_supervision`): Micro 813 860 B, Server 16 145 132 B. Nessun aumento dei budget proposto.

## 5. Casi limite

| Caso | Comportamento |
|---|---|
| Ultima prova a `t0`, tick a `t0+T−ε` | nessuna scadenza (test con tempo controllato `liveness_boundaries_are_never_early`) |
| Tick esattamente a `t0+T` | scade (`>=`) |
| Tick in ritardo | scade al tick servito; ritardo misurato in `watchdog_lateness` / `watchdog_max_late_ns` |
| Osservazione arrivata fra calcolo del token e `apply` | impossibile per costruzione (stesso thread); comunque il marker > token la protegge |
| Osservazione identica con nuova `source_sequence` poco prima della scadenza | marker rinnovato, nessuna scadenza (`renewal_just_before_the_deadline_wins`, `identical_observation_renews_the_marker_and_defeats_an_older_token`) |
| Takeover | operazioni con la generazione vecchia → `Ignored(StaleLinkGeneration)`; il watchdog riparte sul link nuovo |
| Disconnessione | supervisione del link sospesa; nessuna scadenza fino al nuovo collegamento |
| Revisione a `u64::MAX` | `Err(RevisionOverflow)`, atomico (`supervision_revision_overflow_is_atomic`) |
| `operation_sequence` esaurito | errore esistente del sequencer, invariato |
| Sorgente non sorvegliata in config sorvegliata | operazioni rifiutate (`SupervisionNotConfigured`), marker non mantenuti |
| Tick con sequencer saturo | il flag è visto dopo la richiesta corrente; la `Tick` di risveglio non occupa slot dei driver |

## 6. Costi

- Default-off, percorso del batch: un test di flag `supervision.any()` per sorgente nell'`apply` (ramo predetto) e un `Option<Arc<WatchdogShared>>` vuoto nel ciclo del sequencer. Misura: `docs/benchmarks/vs004.md`.
- Attiva: per ogni osservazione di un batch l'aggiornamento di `observed` e lo spostamento in coda della lista (O(oss.)), un tick ogni `poll` che scorre le sorgenti sorvegliate (O(sorgenti sorvegliate) + O(tag che scadono), mai O(tag)), una load atomica relaxed dopo ogni richiesta, un `fetch_add` relaxed per heartbeat nella sessione.

## 7. Compatibilità

| Kernel \ driver | driver VS-003 (1.0) | driver VS-004 (annuncia 1.1) |
|---|---|---|
| kernel VS-003 | invariato | negozia 1.0, **nessun heartbeat inviato** |
| kernel VS-004, supervisione off | invariato, digest v1 | negozia 1.1; heartbeat accettati e ignorati |
| kernel VS-004, supervisione on | negozia 1.0; vivo finché manda batch, altrimenti `Silent` | negozia 1.1; heartbeat = prova di vita |

Le aggiunte Protobuf sono additive (`buf breaking` verde). I file di cattura VS-003 si rileggono invariati; le righe nuove `S`/`R`/`X` compaiono solo con supervisione attiva.

## 8. Prove

`crates/scada-core/tests/supervision.rs` (unit + proptest 512 casi con replay, copertura del generatore), test unitari con tempo controllato in `crates/scada-runtime/src/supervision.rs`, E2E `crates/scada-runtime/tests/vs004.rs` (tutte le matrici di `check.sh`), negoziazione `apps/scada-sim-driver/tests/negotiation.rs`, fuzz `supervision_transitions` e `decode_equivalence` (heartbeat nel decoder), compatibilità con binari reali VS-003 e benchmark: `docs/benchmarks/vs004.md`.

## 9. Conseguenze e rischi

- Non è una garanzia hard real-time: rilevamento fra `T` e `T + poll + ritardo del tick`; sotto saturazione della CPU il ritardo cresce (misurato, non limitato a priori).
- La freschezza non certifica l'età della misura nel dispositivo (§1).
- Un driver legacy su sorgente con liveness che invia dati raramente diventa `Silent` fra un batch e l'altro: scelta di configurazione dell'operatore (timeout > periodo dei dati, o driver 1.1).
- Dopo un silenzio, i tag non osservati dal primo `Delta` restano `Uncertain(SourceSilent)` finché non arriva una loro osservazione: corretto rispetto al brief («solo una nuova osservazione porta Good»), ma un driver che dopo un'interruzione vuole riallineare tutto deve inviare un `FullState`.
- Il thread watchdog esiste anche con il sequencer inattivo: ~`1000/poll` wake-up al secondo (4/s col default).
- Non implementato (fuori scope): timeout per singolo canale, timestamp di acquisizione, allarmi, persistenza, promozione a default.
