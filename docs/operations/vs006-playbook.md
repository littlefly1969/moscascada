# Playbook operativo VS-006 — journal, recovery, tempo

Per chi gestisce un impianto con `[persistence]` attivo (ADR 0013, ADR 0014, contratto §10). Ogni procedura dice **cosa è certo**, **cosa non lo è** e **cosa fare**. Il principio da tenere presente in ogni caso:

> recuperare gli ultimi valori noti ≠ conoscere lo stato del campo adesso ≠ sapere che non sono andati perduti eventi durante l'interruzione.

Strumenti:

| Comando | Uso | Effetto sullo store |
|---|---|---|
| `scada-kernel --config K.toml --check-config` | configurazione, fingerprint, memoria (anche persistenza, % di 4 GiB) | nessuno |
| `scada-journal info <db>` | timeline, `boot_generation`, `durable_operation_seq`/`durable_event_seq`, checkpoint, stato di ogni sorgente (`live` e `continuity`) | sola lettura (sicuro anche col kernel attivo) |
| `scada-journal verify <db>` | come `info` più `PRAGMA integrity_check` completo | sola lettura; con kernel fermo per un risultato stabile |
| `scada-journal history <db> [--after N] [--limit N] [--alarm ID]` | storico allarmi (UTC raw, `durable_at`, `origin`, `reconciliation`) | sola lettura |
| `scada-journal gaps <db>` | intervalli di storia `Gap`/`Unknown` persistiti | sola lettura |
| `scada-journal new-timeline <db> --fingerprint <hex64> --reason <testo> --confirm` | nuova timeline dopo un cambio di configurazione incompatibile | **scrive**; solo con kernel fermo; lo storico precedente resta leggibile |
| socket `scada.history.v1` | `GetJournalStatus`, `GetRecoveryStatus`, `GetTimeStatus`, storico paginato, proiezione attiva | sola lettura |

La riga `READY` del kernel riporta `persistence=full timeline=… boot=N durable_op=… durable_event=… checkpoint_op=… replayed_ops=… recovery_ms=…`; all'arresto (SIGTERM) stderr riporta `persistence: shutdown applied_op=… durable_op=… state=…`.

---

## 1. Blackout / crash del processo o dell'host

**Certo dopo il riavvio:** ogni operazione con `operation_seq ≤ durable_operation_seq` (in particolare ogni batch che ha ricevuto `APPLIED_DURABLE` e ogni ACK operatore con `APPLIED_DURABLE`) è presente; RTDB, allarmi, ACK, storico e checkpoint sono un prefisso coerente.

**Non certo:**
- operazioni confermate solo `APPLIED_VOLATILE` (driver 1.0/1.1 o senza receipt): possono mancare;
- lo stato del campo **adesso**: ogni valore recuperato è `Uncertain(RecoveredUnverified)` finché la sua sorgente non invia un `FullState`;
- gli eventi fisici avvenuti durante il fermo (vedi §5);
- dopo un vero taglio di corrente: la durabilità dipende da disco/controller/hypervisor che rispettano il flush. **VS-006 ha provato kill -9 e una simulazione di WAL troncato, non un taglio di corrente reale.**

**Cosa fare:**
1. Avviare il kernel normalmente. Se la riga `READY` compare, il recupero è riuscito: annotare `durable_op`, `replayed_ops`, `recovery_ms`.
2. Se l'avvio è **rifiutato** (store corrotto, WAL non valido, checkpoint e journal incompatibili) il kernel esce con un messaggio esplicito e **non** riparte volatile. Conservare una copia dei file (`kernel.db`, `kernel.db-wal`, `kernel.db-shm`) prima di qualsiasi intervento, eseguire `scada-journal verify`, poi vedi §3.
3. Verificare che i driver si ricolleghino e inviino un `FullState` (§2). Fino ad allora l'HMI deve mostrare i valori come **recuperati, non verificati** (mai come `Good`).
4. Gli allarmi conservano stato, occorrenza e token; nessun `Returned` o ACK è inventato dal riavvio. I rientri osservati al primo `FullState` sono marcati `reconciliation = true` e `actual_field_time_unknown = true`: il momento reale in campo è ignoto.
5. I driver che non conservano i dati non confermati durevolmente li hanno persi **fuori dal kernel**: va riconciliato con la sorgente (contatori, log dell'apparato).

## 2. Sorgente che non si risincronizza

**Sintomi:** tag della sorgente fermi in `Uncertain(RecoveredUnverified)`; `scada-journal info` (o `GetRecoveryStatus`) riporta per la sorgente `live=UNVERIFIED_RECOVERED` o `AWAITING_FULL_STATE`; il driver riceve `REJECTED` + `RESYNC_REQUIRED` sui `Delta`.

**Certo:** il valore mostrato è l'ultimo **durevole**, non quello attuale. Nessun allarme della sorgente può rientrare per qualità incerta (`input_valid = false`, latch invariato).

**Cosa fare:**
1. `AWAITING_FULL_STATE` (driver collegato, nessun `FullState`): il driver deve inviare un `FullState` con `source_sequence = last_accepted_source_sequence + 1` (il valore è nel `Welcome` e nel suggerimento di ogni rifiuto). Un driver che invia solo `Delta` dopo un riavvio del kernel va corretto: il `Welcome` riporta `full_state_required = true`.
2. `UNVERIFIED_RECOVERED` / sorgente offline: ripristinare la comunicazione; il kernel non «scade» né cancella il valore recuperato (nessuna cancellazione automatica dell'ultimo valore noto).
3. Le altre sorgenti sono indipendenti: una sorgente che rientra diventa `LIVE_VERIFIED` da sola.

## 3. Schema, store o configurazione incompatibili

**Sintomi:** avvio rifiutato con uno di: «… is not a MoscaSCADA journal» / «schema version N not supported», `integrity_check` fallito, «the current timeline of … was created for configuration X but this configuration is Y», «no valid checkpoint … and the journal no longer starts at operation 1», «… the replay payloads before it were compacted», «… is held by another process (one writer per store)», «journal directory …: mode … is writable by group or others» / «owned by uid …».

**Cosa fare, per causa:**
- **Un altro kernel sta scrivendo lo store** (lock): un solo writer per store è un invariante. Fermare l'altra istanza; non copiare né cancellare il file di lock.
- **Permessi:** la directory dello store non deve essere scrivibile da gruppo/altri né un symlink; i file sono `0600` del proprietario del processo. Correggere i permessi, non disattivare il controllo.
- **Configurazione cambiata** (tag, sorgenti, regole: fingerprint diverso): lo stato recuperato non è applicabile alla nuova configurazione. Due scelte:
  1. ripristinare la configurazione precedente (il fingerprint è stampato da `--check-config` e dall'errore), oppure
  2. avviare una **nuova timeline** a kernel fermo: `scada-journal new-timeline <db> --fingerprint <fingerprint nuovo> --reason "<motivo, chi, quando>" --confirm`. Lo stato **non** è recuperato sulla nuova timeline (si riparte con tag `Unknown`); lo storico della timeline precedente resta leggibile; il motivo è registrato.
- **Corruzione** (integrity check, digest errato, buco di sequenza, checkpoint tutti invalidi): conservare i file, `scada-journal verify`; non esiste riparazione automatica. Le opzioni (ripristino da backup dei file a kernel fermo, nuova timeline) sono decisioni dell'operatore e vanno registrate.
- **Versione schema più recente** del binario (downgrade): usare un binario che conosce lo schema; nessuna migrazione all'indietro.

## 4. Disco pieno o lento, errore I/O

**Sintomi:** driver che ricevono `REJECTED` con `DISK_FULL` (46), `JOURNAL_QUEUE_FULL` (45) o `JOURNAL_FAULT` (47); ACK operatore `JOURNAL_UNAVAILABLE`; `GetJournalStatus` con `state` `DISK_FULL`/`FAULT`, `backlog_operations` in crescita, `refused_*` in aumento; messaggi su stderr.

**Certo:** nulla è applicato senza spazio nella coda journal (ammissione **prima** di `apply`); nessuna receipt durevole è inviata senza commit; nulla di durevole è cancellato per fare spazio. La diagnostica resta interrogabile anche con outbox pieno.

**Cosa fare:**
- **`DISK_FULL`** (spazio libero sotto `min_disk_free_bytes`, DB oltre `max_db_bytes`, o `SQLITE_FULL` al commit): liberare spazio sul filesystem dello store (altri file) o ampliarlo. Il writer riprova lo stesso gruppo da solo: appena c'è spazio il commit riesce, le receipt in attesa partono e l'ammissione riprende, **senza riavvio**. Non cancellare righe dal DB a mano.
- **Crescita del DB**: lo storico allarmi non è mai potato automaticamente. I payload di replay possono essere compattati in modo esplicito con `compact_replay_payloads = true` (solo quelli coperti dal più vecchio di almeno due checkpoint, con riga in `retention_markers`); dimensionare il disco con `scripts/alarm_capacity.py --disk-free-mib … --bench-results docs/benchmarks/results/vs006/journal.txt`.
- **`JOURNAL_QUEUE_FULL`** (disco lento o rate oltre la capacità misurata): è backpressure, non perdita. I driver devono ritentare la stessa `source_sequence`. Se persiste, il rate dell'impianto supera la capacità del journal su questo storage (vedi `docs/benchmarks/vs006.md`): ridurre il rate o cambiare storage.
- **`JOURNAL_FAULT`** (EIO o errore non recuperabile): nessun altro commit; le receipt in attesa diventano `APPLIED_NOT_DURABLE`. Controllare il disco (`dmesg`, SMART), poi **riavviare** il kernel: il recupero riparte dall'ultimo prefisso durevole. Le operazioni `APPLIED_NOT_DURABLE` vanno riconciliate con le sorgenti.

## 5. Intervallo di storia sconosciuto

**Sintomi:** `scada-journal gaps <db>` o `GetRecoveryStatus` mostrano per una sorgente `continuity=UNKNOWN` o `GAP`; eventi con `reconciliation = true`.

**Significato:**
- `UNKNOWN` — il kernel non sa se nel campo sono avvenuti eventi durante il fermo (driver senza buffer di eventi verificabile, oppure produttore riavviato con epoca nuova: `bounds_unknown`). La contiguità dei batch **non** prova l'assenza di eventi.
- `GAP` — batch mancanti con intervallo di `source_sequence` noto.
- `COMPLETE` — non viene mai dichiarato per i driver attuali (serve un buffer di eventi della sorgente verificabile: slice futura).

**Cosa fare:** trattare l'intervallo come **storia sconosciuta** nei report (non «nessun evento»). Recuperare la storia dall'apparato (registro eventi della centrale, log del PLC) se esiste. I gap non si cancellano quando la comunicazione riprende: restano come prova.

## 6. Orologio dell'host sbagliato o non sincronizzato

**Sintomi:** `GetTimeStatus` con `clock_regressions` / `jumps_forward` > 0, qualità `UNKNOWN`; righe storiche con `clock_regressed = true`; stderr `ClockRegression`.

**Certo:** l'ordine di operazioni ed eventi è quello di sequenza, mai un timestamp; `logical_time` non diminuisce (neppure dopo un riavvio con RTC arretrato); i tempi raw sono conservati come osservati; i watchdog usano il clock monotono e non sono influenzati dagli step.

**Non certo:** che `sequenced_at` sia l'ora vera. Senza `host_sync_probe` configurato la qualità è `UNKNOWN` (VS-006 non dichiara sincronizzazione senza prova).

**Cosa fare:** correggere la sincronizzazione dell'host (chrony/NTP), configurare `[time] host_sync_probe = "chronyc"` o `"timedatectl"`; nei report usare `sequenced_at` raw con la sua qualità e i flag, e `logical_time` solo come chiave d'ordine. DST: lo storico è in UTC; l'ora locale è una conversione di visualizzazione (le due `02:30` del ritorno all'ora solare hanno offset diversi).

## 7. Cose da non fare

- Non avviare con `durability = "normal"` in esercizio: non è durevole (richiede un flag di test ed è etichettato `NOT_DURABLE_BENCHMARK_ONLY`).
- Non usare `--test-*` in esercizio (rifiutati comunque senza `[persistence]`).
- Non modificare o cancellare righe del DB a mano; non copiare il solo `kernel.db` senza `-wal` a kernel attivo (la copia sarebbe incoerente).
- Non mostrare valori `RecoveredUnverified` come `Good` nell'HMI.
