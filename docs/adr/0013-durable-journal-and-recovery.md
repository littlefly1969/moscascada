# ADR 0013 — Journal durevole, historian, recovery e riconciliazione delle sorgenti

- **Stato:** **proposto per review indipendente** (VS-006). Scritto **prima** delle modifiche strutturali (brief VS-006 §8 passo 2); le sezioni «Verifica» rimandano alla matrice T01–T30 di `docs/traceability.md` (VS-006), compilata a implementazione conclusa.
- **Data:** 2026-11-01
- **Base:** `main` `9f335df` (merge della PR #5, VS-005).
- **Ambito:** nuovo crate `scada-journal` (SQLite), `scada-core` (checkpoint completo, `RecoveryFence`, qualità `RecoveredUnverified`, modalità recuperabile), `scada-runtime` (`[persistence]`, admission, writer, recovery, durable receipt, historian socket), wire additivi (`scada.driver.v1` minor **1.2**, campi additivi `scada.alarm.v1`, nuovo namespace `scada.history.v1`).
- **Compagno:** ADR 0014 (Time Authority e provenance dei timestamp).
- **Default:** sezione `[persistence]` **assente ⇒ comportamento VS-005 volatile**, nessun file aperto, nessun thread in più, stessi fingerprint/digest/ACK (J10).

## 1. Contesto e principio

VS-005 ha fissato l'automa degli allarmi, l'outbox bounded e il contratto del watermark, ma con un sink **solo di test** (`--test-simulated-durable-sink`) che sposta il watermark **senza fsync**. Senza journal ogni slot dell'outbox è consumato per sempre e qualsiasi crash perde RTDB, allarmi e ACK.

VS-006 fornisce un commit durevole reale e un recupero coerente. Il principio, da mantenere visibile in codice, API e documentazione:

> recuperare gli ultimi valori noti ≠ conoscere lo stato del campo adesso ≠ sapere che non sono andati perduti eventi durante l'interruzione.

Il journal ripristina **l'ultimo stato durevole**. Non certifica lo stato attuale del campo né l'assenza di eventi fisici persi durante il fermo.

## 2. Invarianti J1–J12 (brief §1) e dove si realizzano

| # | Invariante | Meccanismo (questo ADR) |
|---|---|---|
| J1 | Autorità del commit | §D2 stati distinti; `durable` = `COMMIT` restituito da SQLite WAL con `synchronous=FULL` (§D1); solo allora `durable_operation_seq` avanza e si genera `AdvanceDurableWatermark` |
| J2 | Totalità causale | una operazione applicata = un record `journal_operations` + i suoi eventi + aggiornamenti di proiezione + metadati temporali + eventuale azione operatore, **nella stessa transazione** (§D5); il recupero è checkpoint + replay del prefisso (§D8) |
| J3 | Ring protetto | il watermark del kernel avanza **solo** a `max event_seq` coperto da un `COMMIT` riuscito (§D5); nessun altro produttore di `AdvanceDurableWatermark` in modalità persistente; sink di test mutuamente esclusivo (§D14) |
| J4 | Ammissione prima della mutazione | prenotazione nella coda journal (operazioni **e** byte, worst case per sorgente) **prima** di `Kernel::apply`; rifiuto tipizzato altrimenti (§D4); anche le operazioni a zero eventi sono registrate |
| J5 | Recovery ≠ verifica del campo | `RecoveryFence` porta ogni tag con valore a `Uncertain(RecoveredUnverified)` e ogni sorgente a `unverified`; serve un `FullState` per tornare `Good` (§D9) |
| J6 | Nessun falso evento | replay in kernel isolato senza I/O; il fence non valuta transizioni (nessun `Raised/Returned/Acknowledged`); i rientri osservati dopo il resync portano `reconciliation=true` (§D9) |
| J7 | Tempo non causale | ordine = `(timeline_id, operation_seq, event_seq)`; ADR 0014 |
| J8 | Buco storico esplicito | `HistoryContinuity` persistita per sorgente/epoca; `FullState` certifica solo il live (§D10) |
| J9 | Recupero bounded | checkpoint periodici, replay solo del suffisso, ring ricaricato con ≤ C eventi, query paginate per keyset (§D7, §D12) |
| J10 | Compatibilità | `[persistence]` assente: nessun cambiamento; evoluzioni wire additive e negoziate (minor 1.2); `APPLIED_VOLATILE` mai reinterpretato (§D11) |
| J11 | Operatori e identity | `actor_uid` resta uid di processo; `action_id` idempotente, nota bounded, `principal` esterno opzionale mai dedotto dall'uid (§D11) |
| J12 | Zero falsi garanti end-to-end | nessun evento fisico ricostruito da un `FullState`; `SourceEventEnvelope` definito ma ingest nativo differito (§D10) |

## 3. Opzioni considerate

### 3.1 Motore di persistenza

| Opzione | Pro | Contro | Esito |
|---|---|---|---|
| WAL binario custom (append + fsync + checksum) | throughput potenzialmente massimo, formato su misura | atomicità multi-record, recovery di code tronche, compattazione, indici e query da scrivere e **provare** da zero | **scartata** in questa slice (brief §2.1) |
| RocksDB / LMDB | maturi, veloci | dipendenza C++ pesante (RocksDB) o modello mmap con limiti di dimensione (LMDB); query storiche da costruire a mano | scartate |
| **SQLite WAL, `synchronous=FULL`** | transazioni ACID, recovery del WAL verificato da 20+ anni, indici e query SQL, un solo file, toolchain C già presente | throughput limitato da fsync per commit; un solo writer | **scelta**; si misura prima di ottimizzare (§D15) |

SQLite è incluso con la feature `bundled` di `rusqlite` (versione nota e fissata nel lockfile, stesse opzioni di compilazione su ogni host). La garanzia di durabilità dipende **anche** dall'onestà di controller, disco, firmware e barriere del filesystem: un disco che dichiara completato un flush senza averlo fatto rende falsa qualsiasi garanzia software. NFS, NAS, replica e HA sono fuori ambito.

### 3.2 Recupero della RTDB

- (a) Proiezione RTDB in una tabella aggiornata a ogni commit: lettura immediata, ma un `UPDATE` per tag modificato in ogni operazione nel percorso di commit. Scartata per ora (costo per tag, nessun bisogno di query SQL sulla RTDB in questa slice).
- (b) **Scelta: checkpoint completo + replay verificato del suffisso.** Lo stato recuperato è quello del kernel, ricostruito deterministicamente; nessun lettore vede una proiezione disallineata perché non esiste una seconda proiezione RTDB.

### 3.3 Il watermark durevole è anch'esso un'operazione

`AdvanceDurableWatermark` consuma un `operation_seq` (VS-005). Alternative:

- (a) non registrarlo: il journal avrebbe buchi di `operation_seq`, il replay dovrebbe inventare sequenze. **Vietato** (brief §2.2).
- (b) metadati separati dal flusso: replay più complesso (ricostruire quando applicare ogni avanzamento).
- (c) **Scelta: registrarlo come operazione ordinaria a zero eventi.** La ricorsione termina: un'operazione di watermark non produce eventi, quindi il suo commit non sposta `durable_event_seq` e non ne genera un'altra. Il journal resta un prefisso **senza buchi** di tutte le operazioni applicate e il replay è esatto (stesso watermark, stessi slot liberi, nessun `OutboxCapacityExceeded` nel replay perché l'esecuzione originale non l'aveva).

## 4. Decisioni

### D1 — Store

- Un file SQLite (`[persistence] path`), directory privata (stessi controlli dei socket: non symlink, proprietario processo o root, non scrivibile da gruppo/altri), file creati `0600`.
- `PRAGMA journal_mode=WAL`, `synchronous=FULL` (unica policy che porta l'etichetta `POWER_LOSS_DURABLE`), `foreign_keys=ON`, `application_id` dedicato, `user_version` = versione schema.
- **Un solo writer**: lock esclusivo (`flock`) su `<path>.lock` per tutta la vita del processo; un secondo `scada-kernel` sullo stesso store fallisce all'avvio (fail-closed).
- `durability` accetta solo `"full"` in produzione. `"normal"` esiste **solo** per i benchmark comparativi, richiede `--test-allow-non-durable` e cambia l'etichetta in `NOT_DURABLE_BENCHMARK_ONLY` ovunque (stderr, `GetJournalStatus`, READY); non abilita mai ACK durevoli.

### D2 — Stati di un'operazione (glossario normativo)

| Stato | Significato | Chi lo vede |
|---|---|---|
| `admitted` | spazio prenotato nella coda journal (§D4) | nessuno |
| `applied RAM` | `Kernel::apply` ha restituito `Applied`; RTDB e allarmi in RAM aggiornati | letture live, ACK `APPLIED_VOLATILE` (legacy) |
| `enqueued` | record consegnato al writer | metriche |
| `written` | statement eseguiti dentro una transazione aperta | nessuno (transazione non ancora visibile) |
| `COMMIT returned` | SQLite ha restituito `SQLITE_OK` dal `COMMIT` con `synchronous=FULL` | writer |
| `durable` | come sopra: con WAL + FULL il `COMMIT` restituisce dopo l'fsync del WAL. È l'**unico** evento che pubblica `durable_operation_seq`/`durable_event_seq` | historian, receipt durevoli, watermark |
| `watermarked` | `AdvanceDurableWatermark{up_to}` applicato dal sequencer | outbox (slot riusabili) |

Una scrittura nella cache del SO non è mai presentata come durevole. Con `durability="normal"` (solo bench) `COMMIT returned` **non** implica `durable` e nessuna receipt durevole è emessa.

### D3 — Timeline e identità persistenti

- `store_uuid` (16 B casuali alla creazione) e `timeline_id` (16 B casuali alla creazione della timeline) in `schema_meta`.
- `operation_seq` ed `event_seq` **continuano** dopo un riavvio dall'ultimo prefisso durevole: il kernel ripristinato riprende da `last_operation_sequence = durable_operation_seq` e `next_event_seq = durable_event_seq + 1`.
- **Un identificativo visto solo come volatile prima del crash non è identità durevole.** Un `operation_seq` ACKato `APPLIED_VOLATILE` ma non durevole può essere riassegnato a un'altra operazione dopo il crash. Identità durevole = `(timeline_id, operation_seq ≤ durable_operation_seq)`. Il `kernel_instance_id` resta nuovo a ogni avvio e invalida token e cursori volatili; ogni avvio è registrato in `runtime_boots(boot_generation, kernel_instance_id, os_boot_id, first_operation_seq, started_at)`.
- Una nuova timeline (configurazione incompatibile, ripristino manuale) è creata **solo** dallo strumento offline con conferma esplicita; lo storico della timeline precedente resta interrogabile.

### D4 — Admission e coda journal (prima di `apply`)

Due code bounded e distinte:

1. l'**outbox** VS-005 (eventi in RAM, slot protetti dal watermark);
2. la **coda journal** (record di operazione in viaggio verso il writer), con due limiti: `journal_queue_operations` (un'operazione a zero eventi conta comunque) e `journal_queue_bytes`.

Per ogni input che può essere applicato il sequencer prenota, **prima** di `Kernel::apply`, `1` operazione e `B_max(op)` byte:

```
B_max(op) = RECORD_FIXED + payload(op) + TIME_ENVELOPE + fanout_max(op) × EVENT_RECORD_BYTES
```

`fanout_max` è il fan-out massimo **della sorgente** dell'operazione (tutte le regole sui suoi tag, calcolato una volta all'avvio), 1 per un ACK, 0 per il watermark; `RecoveryFence` non produce eventi. È un bound conservativo: dopo `apply` la prenotazione si riduce ai byte effettivi. Il massimo utilizzabile e la percentuale di rifiuti dovuti alla stima sono misurati (§D15). Se la prenotazione non entra:

| Origine | Effetto (nulla applicato, nessun `operation_seq` consumato) |
|---|---|
| batch driver | ACK `REJECTED` + `ERROR_CODE_JOURNAL_QUEUE_FULL` (45) / `DISK_FULL` (46) / `JOURNAL_FAULT` (47); il driver ritenta la stessa `source_sequence` |
| Hello | `HandshakeRejected` con lo stesso codice |
| disconnessione | differita e ritentata dopo ogni avanzamento durevole (come l'outbox pieno in VS-005) |
| watchdog | operazione non applicata, ridecisa al tick successivo |
| ACK operatore | esito `JOURNAL_UNAVAILABLE` con causa |
| watermark | rimandato al prossimo avanzamento |

La coda è un `sync_channel` di capacità `journal_queue_operations` (nessun buffer illimitato nascosto) e un contatore atomico di byte prenotati; `try_send` dopo una prenotazione riuscita non può fallire (se fallisse sarebbe un bug: `journal_fault`, admission stop). Il sequencer **non attende mai** il writer né l'fsync.

### D5 — Writer e group commit

- Thread dedicato `scada-journal`, unico proprietario della connessione di scrittura; statement preparati e riutilizzati.
- Riceve i record in ordine di `operation_seq` (un solo produttore, canale FIFO). Raggruppa fino a `commit_max_operations` record o fino a `commit_max_delay_ms` dal primo (limite di batching, **non** promessa di latenza) e li scrive in **una** transazione `BEGIN IMMEDIATE … COMMIT`. Un'operazione e il suo fan-out non sono mai divisi fra transazioni.
- Contenuto della transazione per ogni operazione: riga `journal_operations` (payload canonico versionato con checksum BLAKE3 troncato), righe `alarm_events` (append-only), upsert `alarm_active_projection`, righe `operator_actions` (se ACK con `action_id`), anomalie temporali; poi `projection_meta.as_of_event_seq`, riga `journal_commits`.
- Dopo `COMMIT` riuscito: pubblica `durable_operation_seq` / `durable_event_seq` (atomici), invia le receipt durevoli in attesa, sveglia il sequencer. Il sequencer, alla prima occasione, applica `AdvanceDurableWatermark{durable_event_seq}` (registrato a sua volta, §3.3). Gli slot dell'outbox si liberano quindi **solo** per un prefisso continuo già committato.
- `durable_at` di un commit è osservato dopo il `COMMIT` e scritto nella transazione successiva (`journal_commits.durable_at_utc_ns`); è una metrica di lag, mai il tempo dell'evento (ADR 0014).

### D6 — Schema (versione 1)

Tipi interi espliciti, nessun timestamp testuale nel percorso caldo. Tutte le tabelle sono chiavate per `timeline_id` (intero locale `timeline_no` per compattezza, mappato in `timelines`).

| Tabella | Chiave | Contenuto | Invariante |
|---|---|---|---|
| `schema_meta` | singola riga | `schema_version`, `store_uuid`, `current_timeline`, `boot_generation`, `last_logical_time_ns` | versione sconosciuta ⇒ fail-closed |
| `timelines` | `timeline_no` | `timeline_id` (16 B), `created_at`, `config_fingerprint`, `reason` | — |
| `runtime_boots` | `(timeline_no, boot_generation)` | `kernel_instance_id`, `os_boot_id`, `first_operation_seq`, `started_at_utc_ns`, `recovered_from_checkpoint` | audit dei riavvii |
| `journal_operations` | `(timeline_no, operation_seq)` | `kind`, payload canonico (BLOB), `payload_digest`, `first_event_seq`, `event_count`, tempi (ADR 0014), `reconciliation` | prefisso gap-free; verificato al recovery |
| `alarm_events` | `(timeline_no, event_seq)` | `operation_seq`, `ordinal`, `alarm_id`, `occurrence_id`, `activation_token`, `kind`, `status_after`, `actor_uid`, `reconciliation`, `origin_ts?` | append-only, mai `UPDATE`/`DELETE` salvo retention marcata |
| `alarm_active_projection` | `(timeline_no, alarm_id)` | stato dopo l'ultimo evento, occurrence, token, `last_event_seq` | `as_of_event_seq ≤ durable_event_seq` |
| `projection_meta` | `timeline_no` | `as_of_event_seq`, `as_of_operation_seq` | aggiornata nella stessa transazione |
| `journal_checkpoints` | `(timeline_no, checkpoint_id)` | `operation_seq`, `event_seq_high`, `config_fingerprint`, `state_digest`, `schema_version`, BLOB, `blob_digest`, `complete` | pubblicabile solo con `complete=1` nella stessa transazione |
| `journal_commits` | `commit_id` | `first_op`, `last_op`, `events`, `bytes`, `commit_ns`, `durable_at_utc_ns` (lag) | metriche |
| `operator_actions` | `action_id` (16 B, UNIQUE) | `operation_seq`, `event_seq`, `alarm_id`, `occurrence_id`, `actor_uid`, `principal?`, `note?` (≤ 256 B), `outcome` | dedup retry |
| `source_recovery` | `(timeline_no, source_id)` | `source_epoch`, `boot_generation`, `live_verification`, `history_continuity`, `last_verified_operation_seq`, `last_source_sequence` | live ≠ history |
| `source_history_gaps` | `gap_id` | `source_id`, `source_epoch?`, `from_seq?`, `to_seq?`, `from_op`, `to_op?`, `bounds_unknown`, `reason`, `confidence`, `evidence`, `persisted_at` | mai cancellati da un `FullState` |
| `source_events` | `(timeline_no, source_id, source_epoch, source_event_seq)` | `SourceEventEnvelope` (§D10), ingest nativo differito | schema pronto, nessun writer in VS-006 |
| `time_observations` | `(timeline_no, operation_seq)` | anomalie e cambi di qualità del clock (ADR 0014) | nessuna perdita del raw |
| `retention_markers` | `marker_id` | tabella, range tagliato, motivo, `persisted_at` | taglio mai silenzioso |

Indici: `alarm_events(timeline_no, alarm_id, event_seq)`, `alarm_events(timeline_no, occurrence_id)` via `(alarm_id, occurrence_id, event_seq)`, `journal_operations` per chiave primaria. Nessun indice per tempo di sorgente nel percorso di commit (le query per intervallo di tempo usano `sequenced_at` con indice parziale opzionale, ADR 0014).

### D7 — Checkpoint

- Ogni `checkpoint_every_operations` operazioni applicate il sequencer esporta un `KernelCheckpoint` **completo** (non il `LogicalSnapshot` VS-005, che non basta): per ogni tag valore, qualità, revisione, marker di osservazione; per ogni sorgente link, epoca, `last_accepted`, continuità, ultimo batch canonico (dedup), silenzio, `last_batch_op`, `unverified`; per ogni regola **tutti** gli `AlarmState` (anche `Normal`); `next_event_seq`, watermark, high water, `last_alarm_change_operation`; `last_operation_sequence`. Indici derivati (liste di freschezza, finestre quiet, bitset `listed`) **non** sono salvati: si ricostruiscono.
- L'esportazione avviene sul sequencer fra due operazioni (consistente a `operation_seq` K per costruzione), costa O(tag + regole) di copia; la pausa è misurata (§D15). La serializzazione e il calcolo del digest avvengono sul writer.
- Il checkpoint è accodato **dopo** il record K: il writer lo scrive in una transazione propria solo quando tutte le operazioni ≤ K sono durevoli. `complete=1` è scritto nella stessa transazione del BLOB; un checkpoint interrotto non è mai visibile. Si conservano gli ultimi due checkpoint completi.
- Il BLOB ha formato versionato (`magic`, versione, lunghezze esplicite) e digest BLAKE3; `state_digest` del kernel è salvato a parte e riverificato dopo il caricamento.

### D8 — Recovery all'avvio (fail-closed)

1. Lock esclusivo, apertura, `PRAGMA quick_check` (configurabile `startup_check = "quick" | "full"`), `application_id`, `schema_version`. Versione ignota, migrazione incompleta, integrità fallita ⇒ **uscita con errore**, mai avvio volatile silenzioso.
2. `config_fingerprint` della timeline corrente ≠ fingerprint della configurazione ⇒ fail-closed con messaggio esplicito (serve una nuova timeline, §D3).
3. Ultimo checkpoint `complete` con digest BLOB valido e `state_digest` riverificato; se corrotto si prova il precedente; se nessuno, replay dalla genesi solo se il journal contiene ancora l'operazione 1; altrimenti fail-closed.
4. Replay delle operazioni successive in un **kernel isolato** (nessun socket aperto, nessun HMI, nessun effetto esterno): ogni record deve dare `Applied` con lo stesso `operation_seq` e **gli stessi eventi** registrati (confronto record per record); qualsiasi divergenza, record con digest errato o buco di sequenza ⇒ fail-closed.
5. Ricostruzione degli indici derivati (liste di freschezza, finestre quiet VS-005, bitset) e ricarica del ring con gli ultimi `min(C, durable_event_seq)` eventi dal DB (bounded da C). Nessun replay di milioni di eventi attraverso un ring di C slot: gli eventi già archiviati sono durevoli per definizione.
6. `AdvanceDurableWatermark{durable_event_seq}` e `RecoveryFence` come **nuove** operazioni registrate (§D9). Solo dopo: socket aperti e `READY`.

Il recupero è idempotente: un crash durante il recovery o durante il checkpoint lascia lo store in uno stato da cui il recupero successivo riparte (le operazioni di recovery sono normali operazioni journal; un checkpoint incompleto non esiste).

Recupero manuale distruttivo (nuova timeline, prune): solo strumento offline `scada-journal`, con flag di conferma esplicito, riga di audit in `timelines.reason` / `retention_markers`.

### D9 — Recovery fence, qualità effettiva, nessun falso evento

- Nuova operazione core `RecoveryFence{boot_generation}`, accettata solo da un kernel in **modalità recuperabile** (costruito dal runtime persistente). Effetti, atomici:
  - ogni sorgente `Connected{g}` diventa `Disconnected{g}` (nessun vecchio link sopravvive), `silent=false`, `unverified=true`;
  - ogni tag con valore passa a `Uncertain(RecoveredUnverified)` (nuova causa, precedenza **massima**); il valore resta (`last_known`), la revisione avanza;
  - per ogni regola dei tag toccati: `input_valid=false`, **nessuna valutazione di condizione, nessun evento** (anche `bad_quality` armata resta com'era: lo stato «observability lost all'avvio» è diagnostica di recovery, non un allarme fisico);
  - liste di freschezza vuotate.
- Una sorgente `unverified` richiede `FullState`: `Welcome.full_state_required = true`, un `Delta` riceve `RESYNC_REQUIRED`. Il primo `FullState` applicato riporta i tag `Good`, toglie `unverified` e, nella stessa operazione, valuta le regole: un rientro osservato qui è un evento con `reconciliation=true` e `actual_field_time_unknown=true`, mai retrodatato.
- La precedenza massima di `RecoveredUnverified` impedisce che disconnessioni, silenzio o freschezza successive (prima del resync) lo sovrascrivano o valutino `bad_quality` su uno stato non verificato.
- Il fence è registrato nel journal come ogni operazione: il replay di un riavvio successivo lo riproduce.
- Stati di verifica live per sorgente (runtime, `source_recovery`): `UnverifiedRecovered` → (`AwaitingFullState` dopo l'Hello) → `LiveVerified`; `Offline` se la sorgente non si ricollega; `ReplayingBufferedHistory` riservato all'ingest nativo (differito, §D10). Un heartbeat o un `Hello` non qualificano nulla.
- Timer: nessun confronto di monotonic fra boot; i watchdog VS-004 ripartono dal nuovo `Hello` (nuovo link ⇒ nuovo contatore). Le scadenze avvenute durante il fermo **non sono osservate** e non diventano eventi.

### D10 — Continuità storica (`HistoryContinuity`) e sorgenti con buffer

Per ogni sorgente/epoca: `Complete | Unknown | Gap`, indipendente da `LiveVerification` (tabella brief §3.1).

| Situazione al primo `FullState` dopo il fence | Continuità registrata |
|---|---|
| stessa epoca, `source_sequence = last + 1` (driver legacy 1.0/1.1/1.2 senza buffer) | `Unknown` (`ScadaDown`): la contiguità dei batch non prova che il campo non sia cambiato senza batch |
| stessa epoca, `source_sequence > last + 1` | `Gap` con range di sequenza noto `[last+1, seq−1]` |
| nuova epoca (produttore riavviato) | `Unknown`, `bounds_unknown=true` (`DriverRestart`) |
| sorgente con buffer eventi verificabile | `Complete` **solo** con prova: range progressivi contigui nella stessa epoca, identità evento, nessun hole, resync controllato. Senza prova ⇒ `Unknown`; hole/overflow ⇒ `Gap` |

Il `ReconciliationTracker` implementa la regola per lo stream bufferizzato (idempotente sui duplicati, gap esplicito) ed è provato da test di componente; l'**ingest wire** di eventi nativi (centrali di allarme, antincendio, intrusione) è differito a una slice dedicata. Lo schema `source_events` (`SourceEventEnvelope`: `source_id`, `source_epoch`, `source_event_seq`, `source_event_id?`, `kind`, `source_occurred_at?`, `driver_acquired_at?`, `clock_quality`, `provenance`, payload) è definito ora perché la semantica non la blocchi. Nessun evento è mai inventato da un `FullState`. I gap non si cancellano quando la comunicazione riprende.

### D11 — Receipt durevoli e compatibilità wire

- **`scada.driver.v1` minor 1.2** (additivo, negoziato come 1.1): `Hello.request_durable_receipts`; `Welcome.durable_receipts` (true solo con `[persistence]` attivo e `durability="full"`); `ACK_STATUS_APPLIED_DURABLE = 4` (una sola risposta finale per batch, inviata **dopo** il commit che contiene l'operazione); `ACK_STATUS_APPLIED_NOT_DURABLE = 5` (applicato in RAM, commit fallito: il driver deve riconciliare dopo il riavvio); timestamp sidecar (ADR 0014); codici 45/46/47.
- Un driver 1.0/1.1 (o 1.2 che non chiede receipt) riceve **sempre** `APPLIED_VOLATILE`, che continua a significare «solo RAM», anche se l'operazione sarà poi registrata.
- **`scada.alarm.v1`** (campi additivi, `alarm_api_version` resta 1): `AcknowledgeAlarm.action_id` (16 B), `note` (≤ 256 B UTF-8), `require_durable`; `ACK_STATUS_APPLIED_DURABLE`, `ACK_STATUS_JOURNAL_UNAVAILABLE`. Un retry con lo stesso `action_id` restituisce lo **stesso esito** (stesso `event_seq`) senza nuovo evento; senza `action_id` la semantica resta quella volatile VS-005.
- `principal` autenticato: campo riservato, valorizzato solo da un sistema esterno autenticato (non in VS-006); l'uid non diventa mai un nome utente.

### D12 — Historian e diagnostica (`scada.history.v1`)

Socket UDS separato (`[persistence] history_socket_path`, allowlist uid, connessioni e pagine limitate), servito da un pool bounded di connessioni SQLite **read-only** fuori dal sequencer. Ogni lettura è limitata a `as_of ≤ durable_event_seq` pubblicato.

- `QueryAlarmHistory`: keyset su `(timeline, event_seq)`, filtri per `alarm_id`/occurrence/kind/intervallo di `sequenced_at`, `limit ≤ max_history_query_page`, cursore persistente `(timeline_id, event_seq)`; ogni riga con `sequenced_at`, `durable_at?`, qualità clock, `origin_ts?`, `reconciliation`.
- `GetActiveAlarmProjection`: proiezione persistita con `as_of_durable_event_seq`, distinta dallo snapshot RAM (`scada.alarm.v1`), `recovery_in_progress`.
- `GetRecoveryStatus`, `GetJournalStatus` (watermark, backlog op/byte, fsync p50/p99/p99.9, throughput, checkpoint, WAL, spazio libero, errore persistente, overload; in modalità volatile `DISABLED`), `GetTimeStatus` (ADR 0014).

### D13 — Guasti del disco e retention

- Spazio libero sotto `min_disk_free_bytes` o DB sopra `max_db_bytes` ⇒ admission stop tipizzato (`DISK_FULL`) **prima** di applicare; diagnostica su stderr e `GetJournalStatus` (canale separato dall'historian).
- `SQLITE_FULL` al commit: transazione annullata da SQLite; il writer riprova lo **stesso** gruppo con backoff limitato, nessun avanzamento; admission ferma finché la coda è piena. `EIO` / errore non recuperabile ⇒ stato `FAULT`: nessun altro commit, receipt in attesa → `APPLIED_NOT_DURABLE`, ammissione ferma (`JOURNAL_FAULT`), il processo resta interrogabile; serve un riavvio (che esegue il recovery).
- WAL: `wal_autocheckpoint` di SQLite più `PRAGMA wal_checkpoint(TRUNCATE)` periodico dal writer quando il WAL supera `wal_max_bytes`.
- Retention: le operazioni del journal più vecchie del penultimo checkpoint completo sono rimovibili (servono solo al replay) a blocchi limitati con riga in `retention_markers`; `alarm_events` non è mai potato automaticamente salvo `history_max_events` esplicito, anch'esso con marker. Mai cancellazione come sostituto dell'ammissione.

### D14 — Sink simulato VS-005

`--test-simulated-durable-sink` resta TEST-ONLY ed è **mutuamente esclusivo** con `[persistence] enabled=true` (avvio rifiutato). Con il journal attivo l'unico produttore di `AdvanceDurableWatermark` è il writer reale.

### D15 — Misure obbligatorie prima di ottimizzare

A/B con `[persistence]` assente vs `main`; con persistenza: eventi/s, operazioni/s, fsync/s, latenza `apply → durable` p50/p99/p99.9, pausa di checkpoint, startup e recovery dopo milioni di operazioni, byte/evento con indici, crescita del WAL. Tre profili storage: tmpfs (solo diagnostico, **nessuna** prova power-loss), disco della VM, ARM64 Micro (se disponibile, altrimenti **NOT TESTED ON HARDWARE**). `synchronous=NORMAL` solo come confronto etichettato **NON DURABLE**. Se SQLite non regge il rate richiesto si propone un ADR di ottimizzazione **prima** di sostituirlo.

## 5. Review interna (prima del codice)

Rischi esaminati e come il disegno li chiude:

1. *Apply riuscito, enqueue fallito.* Impossibile per costruzione: la prenotazione precede `apply`, il canale ha capacità ≥ prenotazioni. Se il codice violasse l'invariante il writer entra in `FAULT` e l'ammissione si ferma (nessuna operazione successiva in RAM senza journal).
2. *Watermark oltre il durevole.* Solo il writer pubblica `durable_event_seq`, dopo `COMMIT`; il sequencer non può applicare `AdvanceDurableWatermark` oltre quel valore (nessun altro chiamante in modalità persistente).
3. *Storico avanti rispetto allo stato o viceversa.* Una transazione per gruppo di operazioni intere; checkpoint solo dopo il prefisso; recovery = checkpoint + replay con confronto degli eventi.
4. *Ricorsione del watermark.* Termina (operazione a zero eventi), §3.3.
5. *Restart che produce allarmi falsi.* Il fence non valuta condizioni; la qualità effettiva non-Good è persistita nello stato (nessun overlay da rispettare in ogni lettore: la cella stessa è `Uncertain(RecoveredUnverified)`).
6. *Riuso di identità volatili.* Dichiarato (§D3); il `kernel_instance_id` nuovo lo rende rilevabile.
7. *Blocco del sequencer su fsync.* Il sequencer non attende; rifiuta.
8. *Checkpoint pesante.* Copia O(stato) sul sequencer misurata; se la pausa supera il gate dichiarato si passa a snapshot a blocchi (estensione).

## 6. Compatibilità

- `[persistence]` assente: nessun nuovo file, thread o socket; fingerprint e digest v1/v2/v3 byte-identici; ACK 1.0/1.1 invariati (test golden).
- `[persistence]` presente: digest di stato **v4** (= corpo della versione base + sezione recovery: flag `unverified` per sorgente; la qualità `RecoveredUnverified` ha codice 4), fingerprint invariato.
- `buf breaking` contro la baseline: verde (solo campi/valori additivi).

## 7. Garanzie (e non-garanzie) di VS-006

| Garantito | Non garantito |
|---|---|
| Ogni operazione con receipt durevole (o con `operation_seq ≤ durable_operation_seq`) sopravvive a kill -9 e crash del processo | sopravvivenza al power loss con dischi/controller che mentono sul flush; NFS/NAS |
| Recupero di un prefisso coerente RTDB ↔ allarmi ↔ ACK ↔ historian ↔ checkpoint | che il campo sia ancora nello stato recuperato |
| Nessun falso `Good`, `Returned`, ACK o continuità dopo il riavvio | che non siano andati persi eventi fisici durante il fermo |
| Rifiuti tipizzati prima della mutazione a coda/disco pieni | latenza durevole bounded (dipende dallo storage; è misurata, non promessa) |
| Storico interrogabile oltre la dimensione del ring | exactly-once verso sistemi esterni (fuori ambito) |

## 8. Verifica (a implementazione conclusa)

Le prove sono nella matrice T01–T30 di [`docs/traceability.md`](../traceability.md) (sezione VS-006) e i numeri grezzi in [`docs/benchmarks/vs006.md`](../benchmarks/vs006.md) / `docs/benchmarks/results/vs006/`. Qui si registrano solo gli **scostamenti** dal disegno scritto prima del codice; ogni scostamento è deliberato e dichiarato.

1. **Retention (D13) implementata in forma ridotta.**
   - Implementato: `[persistence] compact_replay_payloads` (default **false**, opt-in). Dopo un checkpoint riuscito il writer svuota i *payload di replay* di `journal_operations` fino all'operazione del più vecchio dei **≥ 2** checkpoint conservati, a blocchi di 20 000 righe (al più 8 blocchi per checkpoint), con riga in `retention_markers` (`table_name = 'journal_operations.payload'`). Le righe (sequenza, envelope temporale, digest) restano; si rimuove solo il corpo necessario al replay.
   - Recovery fail-closed: se nessun checkpoint valido copre il prefisso compattato, l'avvio è rifiutato con un errore `Corrupt` che cita la compattazione (test `replay_payload_compaction_is_explicit_and_fail_closed`).
   - **Non implementato:** cancellazione delle righe di `journal_operations` e `history_max_events` (potatura di `alarm_events`). Lo storico cresce senza limite salvo `max_db_bytes`, che **ferma l'ammissione** (`DISK_FULL`), mai cancella. Dichiarato come limite.
2. **Comportamento di receipt con disco pieno.** Con `SQLITE_FULL` il gruppo viene riprovato con backoff (20 ms → 1 s); i receipt durevoli restano **in attesa** (nessun `APPLIED_DURABLE`, nessun `APPLIED_NOT_DURABLE`) finché il commit non riesce o il writer entra in `FAULT`. Solo `FAULT` produce `APPLIED_NOT_DURABLE`. L'ammissione nel frattempo rifiuta con `DISK_FULL` (46).
3. **Default allineati al brief** (la prima stesura ne aveva di diversi): `journal_queue_operations` 4096 → **65536**; il bound in byte `journal_queue_bytes` (default 64 MiB) resta il limite di memoria effettivo della coda. Il valore non è stato ottimizzato con misure: le tempeste di `vs006.md` §4 girano con questo default.
4. **Chiavi di configurazione diverse dal brief:** non esiste `recovery_mode` (il recovery è sempre fail-closed; il recupero distruttivo è solo nello strumento offline `scada-journal`); la pagina historian si chiama `max_history_page` (non `max_history_query_page`, il nome usato in D12). `[persistence] enabled = false` equivale a sezione assente e fa ignorare anche `[time]`.
5. **Hint d'indice nell'historian.** `QueryAlarmHistory` filtrata per `alarm_id` usa `INDEXED BY alarm_events_by_alarm`: senza l'hint il planner di SQLite percorreva il range della chiave primaria (misurato 100 ms p50 su 6 M righe con 20 000 allarmi). È un accoppiamento esplicito allo schema v1: se l'indice venisse rinominato la query fallirebbe (errore, non risultato sbagliato).
6. **Wire 1.2:** i timestamp di sorgente viaggiano in un campo parallelo di `ObservationBatch` invece che in `Observation` (motivo e misura in ADR 0014 §6).
7. **Non verificato:** power loss reale (solo kill -9 e fault injection del processo), ARM64 hardware (**NOT TESTED ON HARDWARE**), fsync della VM probabilmente assorbito da una write cache dell'hypervisor (~0,23 ms): le latenze durevoli misurate sono un **limite inferiore** rispetto a un disco fisico.
