# EXP-001 — Spike report: open-wal 0.2.1 come journal autorevole di MoScada

- **Stato:** **SPIKE SPERIMENTALE, proposto per review indipendente.** Nessuna promozione a produzione. PR draft #7, base VS-006 `3a409ca` (PR #6, non mergiata). **Non mergiare PR #6 né PR #7 automaticamente.**
- **Specifica:** [`exp-001-open-wal-backend.md`](exp-001-open-wal-backend.md).
- **Data:** 2026-11-02.
- **Codice:** feature `experimental-open-wal` di `scada-journal` (modulo `owal`) e di `scada-bench` (`scada-bench exp001`, `scada-bench fsync-probe`). **Default off.** Nessun file di runtime, kernel, driver protocol, allarmi o Time Authority è stato modificato.
- **Risultati grezzi:** [`docs/benchmarks/results/exp001/`](../benchmarks/results/exp001/) (output verbatim di ogni esecuzione, CSV, ambiente, riepilogo).

> **Esito in breve.**
>
> - **Correttezza: sì.** open-wal 0.2.1 + l'adapter dello spike conserva le invarianti di VS-006 nei test eseguiti. Lo stesso stream dà stato, watermark, historian e proiezioni **identici** a SQLite (T01), e si recupera coerentemente da 12 crash pilotati, 12 SIGKILL casuali, EFBIG ed ENOSPC reali e 60 corruzioni casuali (T02–T04, T11). Restano **non provati**: integrazione nel runtime (T06), power loss reale e hardware ARM/Micro (T12).
> - **Prestazioni: nessun miglioramento end-to-end ripetibile nella configurazione di riferimento.** Con group commit 1024 (il default del runtime) e il carico saturo, **C (open-wal + proiezione SQL) rende l'historian pronto a 0,96× di A** (n = 10, mediana 122 560 contro 127 982 op/s, intervalli sovrapposti). Su 10 M eventi il rapporto è 0,99×; con 16 eventi/op 1,11×; con 128 osservazioni/op 1,15–1,17×. In cambio C usa **+59 % CPU, 4× RSS, 2,3–4,4× disco** e una **recovery 2,5× più lenta** (con la proiezione da riallineare), e il lettore concorrente vede un ritardo p99 di 73 728 eventi contro 1 008.
> - C vince davvero solo con **commit piccoli** (1/16/128 operazioni: 1,25–1,9×) e sulla latenza mediana delle receipt sotto il punto di saturazione (circa −30 % a p50; code p99/p99.9 non migliori). Il vantaggio viene dal **disaccoppiamento**: il proiettore raggruppa molte operazioni in una transazione SQLite senza fsync. Non viene dal log in sé.
> - **B (solo log, nessun historian) è 1,8–13× più veloce di A**, ma non è confrontabile come sistema: è il tetto del log senza indici.
> - **Raccomandazione: mantenere SQLite** come journal autorevole e backend predefinito. **Non promuovere** open-wal e non integrarlo nel runtime ora. Lo spike resta come riferimento riproducibile (feature off). Le condizioni per riaprire la questione sono in §7.

## 1. Che cosa è stato costruito (e che cosa no)

| Fase spec §3 | Stato | Dove |
|---|---|---|
| 1 — adapter bench-only dietro feature | **fatto** | `crates/scada-journal/src/owal/` (feature `experimental-open-wal`), `apps/scada-bench/src/exp001.rs` |
| 2 — replay verificato, envelope versionato | **fatto** | `owal/envelope.rs`, `owal/mod.rs::recover` |
| 3 — materializzazione derivata | **fatto** (SQLite «Micro»; PostgreSQL **non** fatto) | `owal/projector.rs` |
| 4 — prova runtime isolata | **non fatto** (deliberatamente: vedi §8) | — |

Modifiche a codice esistente, tutte senza effetto sul comportamento di default:

- `crates/scada-journal/src/writer.rs`: `write_rows` e `Written` da privati a `pub(crate)`, per riusare **lo stesso** codice SQL nel proiettore (nessuna riga di logica cambiata).
- `crates/scada-journal/src/lib.rs`: `#[cfg(feature = "experimental-open-wal")] pub mod owal;`.
- `Cargo.toml` (workspace), `crates/scada-journal/Cargo.toml`, `apps/scada-bench/Cargo.toml`: dipendenza opzionale `open-wal = "=0.2.1"` e feature opt-in. `cargo tree -p scada-journal` e `-p scada-runtime` **non** contengono open-wal senza la feature (verificato anche in CI).
- `apps/scada-bench/src/main.rs`: due sottocomandi sotto `cfg(feature)`.

## 2. Verifica della crate prima dell'integrazione (spec §2)

| Voce | Esito |
|---|---|
| Versione | `open-wal = "=0.2.1"` (pin esatto) + `Cargo.lock`. Pubblicata 2026-07-03; prima release 0.1.0 il 2026-06-17; **3 versioni in 16 giorni, nessuna da 4 mesi, 174 download totali** (crates.io API, 2026-11-02). Crate **giovanissima**, un solo autore. |
| Licenza | BSD-3-Clause (ammessa da `deny.toml`). Dipendenze nuove: `crc32c 0.6.8` (Apache-2.0/MIT), build-dep `rustc_version 0.4.1`, `semver 1.0.28`; `rustix` già presente. |
| Advisories | `cargo deny check` (0.20.2, `deny.toml` con `all-features = true`, quindi **include open-wal, crc32c, rustc_version, semver**): `advisories ok, bans ok, licenses ok, sources ok` il 2026-11-02 (`docs/benchmarks/results/exp001/cargo-deny.log`). Nessun advisory RustSec noto per le nuove crate a quella data. |
| MSRV | 1.85 ≤ toolchain del repo 1.97. |
| `unsafe` | nessuno in open-wal; `crc32c` usa intrinseci SSE4.2/ARMv8 CRC (`unsafe`, 14 occorrenze). Il nostro codice resta `forbid(unsafe_code)`. |
| Upstream activity | GitHub API non raggiungibile da questo ambiente (403): **non verificata**. |
| Thread / I/O | nessun thread in background; `pwrite` + `fdatasync` (macOS `F_FULLFSYNC`); segmenti preallocati con `fallocate` (fallback scrittura di zeri), header sincronizzato alla creazione, `fsync` della directory dopo creazione/cancellazione segmenti; `flock` esclusivo su `LOCK`. |

API verificate leggendo il **sorgente** 0.2.1 (non solo la documentazione) e con test propri:

- `append` = solo memoria (staging), assegna l'LSN denso successivo; **non c'è un'API per annullare lo staging**: un errore a metà gruppo lascia l'handle in uno stato da abbandonare (noi andiamo in FAULT).
- `commit` = `pwrite` dello staging + `fdatasync`; ritorna il `durable_lsn`. **Non è una transazione:** se il gruppo non entra nel segmento attivo viene spezzato a confine di record, ogni parte ha il suo `fdatasync`, e un errore fra le due lascia durevole solo il prefisso (`commit_splits_batch_across_segments`, documentato in `book/durability-model.md`). Verificato da noi con ENOSPC reale (T04).
- Qualsiasi errore di `write`/`fdatasync`/creazione segmento **avvelena** l'handle (`Poisoned` per ogni chiamata successiva). Non c'è retry possibile: solo `drop` + `Wal::open`.
- `max_record_size` default 1 MiB, vincolo `max_record_size + 91 ≤ segment_size` (validato in `open`); `append` oltre il limite → `RecordTooLarge`, nessuna troncatura, nessuna frammentazione.
- `Wal::open` esegue la recovery e **modifica i file**: azzera durevolmente la coda strappata del segmento attivo; un record invalido con record validi dopo di sé (bounded forward scan) o in un segmento sigillato → `TornMidLog` / `Corruption` (fatale).
- `reader_from(&self, lsn)` prende in prestito l'handle del writer (`Wal` è `!Sync`): **un lettore concorrente in un altro thread non è possibile** con l'API pubblica 0.2.1, né in un altro processo (il `LOCK` è esclusivo e non c'è un lettore read-only pubblico). Conseguenza progettuale in §3.5.
- `checkpoint(up_to)` cancella segmenti interi ≤ `up_to`: **nello spike non è mai chiamato** (retention disabilitata, spec §2.6).

Che cosa garantisce la crate e che cosa resta a MoScada:

| open-wal garantisce | MoScada deve fare |
|---|---|
| durabilità per record dopo `commit` → `Ok(w)`; prefisso denso; CRC-32C per record; torn tail troncato; corruzione a metà log fatale | unità atomica = 1 operazione in 1 record; mapping LSN↔operation_seq; contenuto del record (codec, versione, checksum interno); checkpoint del kernel e del ring; replay verificato; FAULT su poison; admission prima di apply sul limite del record; nessuna cancellazione di segmenti; proiezione e suo watermark |

## 3. Progetto dell'adapter

### 3.1 Unità atomica e formato

**Una operazione applicata = un record open-wal.** Mai un'operazione su più record. L'envelope (`owal/envelope.rs`, versione 1) contiene tutto ciò che la transazione SQLite di VS-006 scrive in più tabelle:

```text
header 16 B : "MSOW" | version u16 = 1 | flags u16 = 0 | body_len u32 | crc32c(body) u32
body        : timeline_id[16] | operation_seq u64 | boot_generation u64
              | TimeEnvelope (sequenced, boottime, logical, clock_quality, clock_flags, received?)
              | reconciliation u8 | op payload (stessi byte canonici di codec::encode_op)
              | source_times? (stessi byte di codec::encode_source_times)
              | events[] (event_seq, operation_seq, ordinal, alarm, occurrence, token, kind, status, actor)
              | origins[] | alarm_updates[] (cambi di stato senza evento, fix PR #6)
              | operator action? (action_id, token, actor, note ≤ 256 B, outcome, event_seq?, occurrence)
              | source recovery updates[] (+ gap) | time anomaly?
```

- Decoder **strict e bounded**: ogni contatore è verificato contro i byte restanti prima di allocare; flag solo 0/1; codici sconosciuti, byte in coda, eventi non contigui, ordinal fuori ordine, origini incoerenti → errore. Un errore in recovery è corruzione (fail-closed). Encoding **canonico** (decode→encode = stessi byte), verificato da proptest e da un fuzz target (`fuzz/fuzz_targets/owal_envelope_decode.rs`).
- Il CRC interno è ridondante con quello di open-wal finché il record resta nel log; protegge l'envelope quando viene copiato fuori (proiettore, futura replica).
- `worst_case_len(observations, max_events, ...)` dà un limite superiore calcolabile **prima** di `Kernel::apply`: con 1 MiB stanno 10 000 eventi in un'operazione (≈ 580 KB), 20 000 no (T05). L'admission del runtime dovrebbe usarlo come la prenotazione J4 di VS-006 usa `queued_bytes`; nello spike il writer che incontra comunque un record troppo grande va in FAULT senza scriverne nulla (T05).

### 3.2 Mapping LSN ↔ operation_seq ↔ event_seq

Su una timeline, **LSN = operation_seq** (entrambi densi da 1; nessun record marker nel log). Il writer verifica che `append` restituisca l'LSN uguale all'operazione (altrimenti FAULT); il replay verifica `record.operation_seq == lsn`, la timeline, la contiguità degli eventi (`event_seq` consecutivi e uguali a quelli rigenerati dal kernel). Boot, checkpoint e identità vivono **fuori** dal log (MANIFEST, file di checkpoint, campo `boot_generation` in ogni record), così il mapping resta l'identità. Una nuova timeline = una nuova directory (come una nuova timeline SQLite è una riga nuova): la migrazione non è automatica.

### 3.3 Commit, watermark, receipt

Il writer (`owal/writer.rs`) ha **la stessa interfaccia** del writer SQLite (`WriterMsg`, `JournalShared`, stesso ciclo di group commit con `commit_max_operations` / `commit_max_delay`). Per gruppo: encode + `append` di ogni operazione, un `commit`, poi — solo dopo il ritorno — `durable_op`/`durable_event`, rilascio delle prenotazioni, receipt, sveglia del sequencer (J1/J3 invariati). Se `commit` fallisce dopo uno split, le operazioni ≤ `durable_lsn` (davvero durevoli, e rigiocate al riavvio) ricevono `Durable`, le altre `NotDurable`, e il writer va in **FAULT** (nessun commit successivo, receipt successive `NotDurable`). Il fault point dei test VS-006 (`before_transaction`, `during_transaction` = dopo il primo append, `after_commit_before_watermark`, `during_checkpoint`) è riusato.

### 3.4 Checkpoint, recovery, retention

- Directory: `MANIFEST` (magic, versione, timeline_id, fingerprint di configurazione, CRC; creato atomico tmp+fsync+rename+fsync dir), `wal/` (segmenti open-wal), `checkpoints/ckpt-<op>.bin`.
- Checkpoint = blob `codec::encode_checkpoint` di VS-006 + **gli eventi della finestra del ring** (open-wal non ha indici da cui ricaricarli) + header (op, event_seq_high, boot, last logical time, state digest) + BLAKE3 di tutto il file; scrittura atomica e durevole; si tengono gli ultimi 2. Un checkpoint fallito non è FAULT (accorcia solo la recovery).
- Recovery: `Wal::open` (troncatura della coda strappata) → checkpoint più recente valido (digest, timeline, fingerprint, finestra del ring, restore, state digest), fallback ai precedenti e poi a genesi → replay verificato di ogni record successivo in un kernel nuovo, confronto evento per evento. Ogni divergenza è `Corrupt`. `Position` restituisce durable op/event, ultimo boot e ultimo logical time (per la regressione cross-boot di ADR 0014).
- **Retention disabilitata:** `Wal::checkpoint` non è mai chiamato. Un log che non parte da LSN 1 o con un segmento mancante è rifiutato (T10). Un log pieno ⇒ disco pieno ⇒ FAULT (vedi §6).

### 3.5 Variante C: proiezione SQL derivata

`owal/projector.rs` materializza **lo stesso schema VS-006** usando **lo stesso** `write_rows` del writer SQLite, in un file SQLite separato (`synchronous=NORMAL`: è derivato e ricostruibile). Il suo watermark è `projection_meta.as_of_operation_seq`, scritto nella stessa transazione SQL delle righe e **distinto** dal watermark del WAL; idempotente su `(timeline, operation_seq)`, una lacuna è corruzione. L'historian (`reader::history`, `projection`, `as_of`) funziona invariato sul file della proiezione e dichiara l'operazione a cui è aggiornato. Il file è legato alla timeline del log (campo `timelines.reason`): un file di un altro log è rifiutato e ricostruito.

Percorso live: dopo le receipt il writer passa i record durevoli al proiettore su un canale **bounded** (64 gruppi). Per la limitazione di `reader_from` (§2) il proiettore **non può** rileggere il log mentre il writer gira: se resta indietro oltre il bound, il canale pieno **blocca il writer** (backpressure; mai perdita, mai salto). Quindi «niente SQL nel percorso di ACK» vale solo finché il proiettore tiene il passo: in saturazione il throughput durevole di C è limitato da quello del proiettore (§5). Il catch-up dal log avviene all'avvio, dopo un crash del proiettore o dopo la perdita del file (rebuild completo, T01/T08, misurato).

## 4. Test di correttezza (spec §4) — eseguiti prima di ogni conclusione prestazionale

`cargo test -p scada-journal --features experimental-open-wal` (log: `docs/benchmarks/results/exp001/tests.log`). Legenda come in `docs/traceability.md`: **PASS** eseguito e superato; **PARTIAL** prova solo una parte; **NOT DONE** non eseguito (motivo).

| # | Esito | Test (`crates/scada-journal/tests/owal.rs`, `src/owal/envelope.rs`) |
|---|---|---|
| T01 equivalenza A vs C | **PASS** | `t01_sqlite_and_owal_projection_are_equivalent`: stesso stream canonico (eventi, ACK con azione operatore e nota, RecoveryFence al 2° boot con cambi di stato senza evento, source recovery con gap, source timestamps, origini, anomalie di clock), due boot, checkpoint ogni 50 op, segmenti da 64 KiB (roll e split reali). Uguali: state digest live e recuperato, durable op/event, **tutte** le tabelle dell'historian riga per riga (journal_operations con payload e tempi, alarm_events, alarm_active_projection, projection_meta, operator_actions, source_recovery, source_history_gaps, time_observations), pagine `reader::history` (escluso `durable_at_utc_ns`, orologio di parete del commit), `projection`, `as_of`. Poi la proiezione è **cancellata e ricostruita dal solo log** e torna identica. B (solo log) è coperto dallo stesso test come autorità di recovery. |
| T02 shutdown/crash/ACK prematuro | **PASS** | `t02_t03_crash_at_every_fault_point`: processo figlio, `abort()` a `before_transaction`, `during_transaction` (dopo il primo append, prima del commit), `after_commit_before_watermark`, `during_checkpoint`, ognuno alla 1ª, 7ª e 40ª occorrenza (12 crash). Verificato: op recuperata ≥ ogni op pubblicata come durevole (nessun ACK prematuro), stato recuperato = digest che il figlio aveva a quell'op, sequenza eventi coerente (nessun duplicato), ripresa sulla stessa timeline. |
| T03 crash casuali e corruzione | **PASS** | `t03_random_sigkill` (12 SIGKILL a 77–397 ms, fino a 3 683 op); `t03_torn_tail_…` (coda strappata troncata di esattamente un record; flip a metà di un segmento sigillato e del segmento attivo ⇒ `Corrupt`); `t03_damaged_checkpoints_fall_back_then_genesis`; `t03_identity_configuration_and_single_writer` (secondo writer `Locked`, altra configurazione `Incompatible`, MANIFEST danneggiato o perso con segmenti presenti `Corrupt`); `t03_projection_ahead_of_a_truncated_log_is_detected`. |
| T04 EIO, split al roll, poison, disco pieno | **PASS** | `t04_simulated_eio_faults_and_recovers`; `t04_real_write_failure_poisons_and_faults` (**EFBIG reale** via `ulimit -f` a metà del segmento preallocato); `t04_real_enospc_at_segment_preallocation` (**ENOSPC reale**: tmpfs da 448 KiB in un mount namespace, la preallocazione del 4° segmento fallisce durante un commit spezzato al roll ⇒ prefisso durevole, FAULT, recovery = stato applicato). Saltato con messaggio se `unshare -rm` non è disponibile. |
| T05 burst e limite del record | **PASS** | `t05_…`: fan-out 1/10/100/1 000/10 000 eventi in un record con il default 1 MiB, recovery uguale; 20 000 eventi superano `worst_case_len` ⇒ rifiuto prima di apply; writer con `max_record_size` 4 KiB e 200 eventi ⇒ FAULT `RecordTooLarge`, nulla dell'operazione nel log. 0 eventi coperto nei benchmark (`shape/s1-o1-e0`). |
| T06 disco lento, WAL bloccato, DUPLICATE | **NOT DONE** | Richiede l'integrazione runtime (fase 4, non fatta). Lato writer la backpressure è la stessa coda bounded di VS-006; non provata qui. |
| T07 ACK idempotente | **PARTIAL** | L'azione operatore (action_id, token, nota, outcome, occurrence) sopravvive identica log→proiezione e rebuild (T01). La logica di dedup è del runtime (non integrato). |
| T08 recovery fence e proiezione senza evento | **PASS** | `t08_projection_follows_status_change_without_event` + T01 (rebuild dal log conserva `status_after` cambiato dal fence). |
| T09 clock -1 s / -1 h / +24 h | **PARTIAL** | Le annotazioni con i tre salti (e flag/anomalie) sono conservate byte per byte e uguali ad A (T01); LSN/op/event_seq monotoni per costruzione e verificati nel replay. DST Europe/Rome non rilevante (solo UTC ns nel log), Time Authority non toccata. |
| T10 log enorme, retention | **PARTIAL** | Retention disabilitata; `t10_…`: segmento intermedio o prefisso cancellati ⇒ `Corrupt`. Replay misurato su 1 M op / 10 M eventi (§5). Milioni di op con consumer arretrato: non provato oltre il bound del canale. |
| T11 fuzz/property | **PASS** | proptest: roundtrip, ogni troncatura e **ogni singolo bit flip** rifiutati, garbage e corpi forgiati con CRC valido senza panic e canonici; `t11_random_segment_corruption…` (60 copie del log con 1–3 flip casuali: 30 prefissi uguali al riferimento, 30 `Corrupt`, mai una bugia né un panic); fuzz target `owal_envelope_decode` (CI nightly, 60 s). |
| T12 cross-platform | **PARTIAL** | `cargo check --target aarch64-unknown-linux-gnu` di open-wal 0.2.1 + crc32c **OK** in questo ambiente. `scada-journal` con la feature non è controllabile qui (SQLite bundled richiede un cross compiler C aarch64, assente); il job CI `exp001-open-wal` lo installa (`gcc-aarch64-linux-gnu`) e controlla `scada-journal --features experimental-open-wal --all-targets` per aarch64. Nessun test su hardware ARM/Micro, **nessuna prova di power loss reale**. |

Due risultati qualitativi dai test, da pesare nella decisione:

1. **Un bit flip nell'ultimo record committato è indistinguibile da una scrittura strappata**: open-wal lo tronca in silenzio (durable op = N-1, nessun errore). Anche il WAL di SQLite ignora un frame finale con checksum errato, quindi non è una regressione di classe; ma qui l'unico rivelatore è esterno (la proiezione «avanti» rispetto al log, testato; un checkpoint oltre il log viene rifiutato e si ricade su uno precedente).
2. **ENOSPC non è recuperabile senza riavvio**: il writer SQLite di VS-006 riprova lo stesso gruppo dopo `SQLITE_FULL` (D13, stato `DISK_FULL`); open-wal avvelena l'handle ⇒ FAULT ⇒ riavvio e recovery. Con segmenti preallocati ENOSPC può avvenire solo al roll; ma l'operatività cambia.

## 5. Benchmark (spec §5)

### 5.1 Metodo e ambiente

- **Una sola harness per le tre varianti** (`scada-bench exp001`, `apps/scada-bench/src/exp001.rs`): stesso kernel reale, stessa configurazione e stesso stream di operazioni. Stessi metadati per record (TimeEnvelope, source timestamps su ogni batch), stesso `commit_max_operations` / `commit_max_delay`, stessa coda bounded (4 096 messaggi), stesso rilascio del ring sul watermark durevole, checkpoint ogni 100 000 op.
- **Durabilità uguale sul percorso autorevole.** A: SQLite WAL `synchronous=FULL` (fsync del `-wal` a ogni COMMIT). B/C: `fdatasync` del segmento preallocato a ogni commit, più `fsync` della directory quando si crea un segmento. La receipt durevole parte, in tutte e tre, solo dopo il ritorno del commit.
- **Funzionalità diverse, dichiarate:** A ha l'historian pronto al commit. C lo ha **quando il proiettore raggiunge il log** (metrica `historian-ready op/s` = operazioni / tempo fino a proiezione allineata); la proiezione è `synchronous=NORMAL` perché derivata. B non ha historian. **Solo A vs C, sulla metrica historian-ready, risponde alla domanda dell'esperimento.**
- **Campagna** (`scripts/exp001_campaign.py`): varianti interleaved a, b, c, a, b, c… nella stessa cella. **10 ripetizioni** per le celle critiche (batch 1/16/128/1024 senza delay), 3 per le celle con delay 1/5 ms, 5 per forme, lettura concorrente e latenza, 3 per 10 M eventi. Mediana [min–max]. Riepilogo completo: [`summary.md`](../benchmarks/results/exp001/summary.md). Output verbatim: `raw/*.txt`. CSV: `exp001-*.csv`.
- **Ambiente** (`environment.txt`): VM KVM, 4 vCPU Intel Xeon 2,1 GHz, 16 GiB, ext4 su disco virtuale `/dev/vda` (`rw,relatime,discard`), Linux 6.18, rustc 1.97.0, open-wal 0.2.1, rusqlite 0.41.1 (SQLite bundled). Il binario è stato compilato dal working tree poi committato in questa PR (il campo `git` di `environment.txt` indica il commit precedente). `fdatasync` misurato sullo stesso filesystem: p50 178–413 µs, p99 0,57–1,2 ms, max 5,3 ms (512 B…64 KiB). **Disco virtuale: nessuna di queste misure vale come certificazione di power loss.** La baseline storica VS-006 non è stata confrontata con questi numeri; A è stato rimisurato qui.

### 5.2 Journal → watermark durevole, batch e delay (1 evento/op, 1 osservazione/op)

Throughput in op/s, mediana [min–max]:

| cella | n | A SQLite | B open-wal solo log | C durevole | **C historian-ready** | **C/A historian** |
|---|---|---|---|---|---|---|
| batch 1 | 10 | 2 794 [2 583–3 126] | 5 768 [4 916–6 999] | 3 819 | **3 819** [3 321–4 184] | 1,37× |
| batch 16 | 10 | 23 253 [21 011–27 568] | 43 196 [35 517–47 606] | 30 555 | **30 522** [25 354–33 782] | 1,31× |
| batch 128 | 10 | 63 808 [61 671–71 701] | 109 086 [101 950–121 698] | 105 900 | **104 866** [73 972–124 633] | 1,64× |
| **batch 1024** | 10 | **127 982** [121 971–137 029] | 234 024 [224 110–241 807] | 164 364 | **122 560** [118 333–132 705] | **0,96×** |
| batch 1 + 1 ms | 3 | 3 217 | 6 284 | 4 022 | 4 021 | 1,25× |
| batch 1 + 5 ms | 3 | 2 861 | 5 221 | 3 761 | 3 760 | 1,31× |
| batch 16 + 1/5 ms | 3 | 25 909 / 23 182 | 44 683 / 40 257 | | 32 856 / 30 236 | 1,27× / 1,30× |
| batch 128 + 1/5 ms | 3 | 65 746 / 70 821 | 110 495 / 130 287 | | 124 660 / 126 787 | 1,90× / 1,79× |
| batch 1024 + 1/5 ms | 3 | 132 159 / 128 838 | 234 597 / 223 768 | | 135 036 / 114 296 | 1,02× / 0,89× |

Byte scritti per operazione (`/proc/self/io`), batch 1 → 1024: A 37 306 → 513, B 4 352 → 290, C 35 516 → 724. A batch piccoli SQLite riscrive pagine intere nel `-wal` a ogni commit, open-wal scrive solo il record. C riscrive comunque nella proiezione, ma in transazioni più grandi e senza fsync. Sotto batch 1024, quindi, **il guadagno di C viene dal disaccoppiamento della proiezione**, non dal formato del log.

### 5.3 Forme di carico (batch 1024, saturo)

| cella (sorgenti × oss/op × eventi/op) | n | A | B | C historian-ready | C/A | CPU A / C (s) | RSS A / C (MiB) | disco A / C (MiB) |
|---|---|---|---|---|---|---|---|---|
| 1 × 1 × 0 | 5 | 190 708 | 266 648 | 198 764 | 1,04× | 0,93 / 1,41 | 17 / 57 | 30 / 158 |
| 1 × 1 × 16 | 5 | 19 540 | 121 611 | 21 784 | 1,11× | 5,05 / 5,06 | 25 / 129 | 87 / 218 |
| 1 × 1 × 100 | 5 | 3 142 | 49 259 | 3 552 | 1,13× | 9,25 / 9,25 | 23 / 203 | 135 / 391 |
| 8 × 128 × 1 | 5 | 18 557 | 33 553 | 21 752 | 1,17× | 1,99 / 2,43 | 46 / 219 | 225 / 481 |
| 16 × 128 × 1 | 5 | 18 072 | 33 500 | 20 823 | 1,15× | 1,97 / 2,51 | 46 / 216 | 225 / 481 |
| 16 × 1 × 16 | 5 | 11 972 | 131 100 | 17 706 | 1,48× | 7,22 / 5,97 | 25 / 130 | 89 / 220 |
| 1 M op × 10 ev (**10 M eventi**) | 3 | 29 258 | 139 132 | 29 068 | **0,99×** | 31,1 / 40,0 | 25 / 130 | 608 / 1 382 |

Con fan-out (16/100 eventi/op) il collo di bottiglia è l'inserimento SQL degli eventi e del loro indice. **È lo stesso codice in A e in C** (`write_rows`), quindi C non può fare meglio di A di più del costo del fsync risparmiato: 1,1–1,5×. B mostra il tetto del log: 2,0 M eventi/s a 16 eventi/op e 4,9 M a 100 eventi/op.

### 5.4 Latenza delle receipt durevoli sotto saturazione (produttore cadenzato, 10 s, batch 1024, n = 5)

Nelle celle sature la «latenza» è accodamento (coda da 4 096 piena). Qui il produttore è sotto il punto di saturazione. Receipt = da `boottime` all'apply fino alla pubblicazione del watermark durevole, in ms:

| op/s | A p50 / p99 / p99.9 | B p50 / p99 / p99.9 | C p50 / p99 / p99.9 |
|---|---|---|---|
| 1 000 | 1,80 / 7,1 / 9,7 | 1,31 / 3,7 / 3,9 | 1,25 / 3,8 / 5,6 |
| 5 000 | 1,54 / 5,8 / 10,5 | 1,08 / 3,6 / 5,9 | 1,05 / 5,8 / 41,9 [6,9–115] |
| 20 000 | 1,31 / 5,6 / 8,4 | 0,93 / 4,6 / 22,0 | 0,93 / 10,5 / 17,3 |

La mediana scende di circa il 30 % (un fsync di file preallocato costa meno di un COMMIT SQLite FULL). **Le code p99/p99.9 di C non sono migliori di A**, e a 5 000 e 20 000 op/s sono peggiori e più disperse: il proiettore compete per CPU e disco, e quando resta indietro il canale bounded rallenta il writer.

### 5.5 Historian, lettura concorrente, recovery, rebuild

- **Pagine da 1 000 eventi dopo il run:** A e C equivalenti (p99 2,0–2,5 ms; 14–16 ms con 128 osservazioni/op in entrambi). Stesso schema, stesse query.
- **Lettura concorrente ogni 10 ms durante la scrittura** (n = 5): latenza p99 A 3,3 ms, C 2,9 ms. **Ritardo dei dati letti rispetto al log durevole: A 1 008 eventi, C 73 728 eventi** (p99). Throughput historian-ready C/A 0,91×. C dichiara l'operazione a cui è aggiornata (`as_of`), ma è molto più indietro.
- **Recovery** (apertura + checkpoint + replay; per C anche riapertura della proiezione e riallineamento): batch 1024 A 143 ms, B 266 ms, C 363 ms. Su 10 M eventi: A 2,79 s, B 1,48 s, C 4,11 s. A paga il `quick_check`; B e C pagano la scansione dei segmenti in `Wal::open`. **Rebuild completo della proiezione dal log: 33,9 s per 1 M op / 10 M eventi** (circa 30 k op/s). È il tempo di indisponibilità dell'historian se la proiezione va persa.
- **Disco:** B/C preallocano 128 MiB per segmento, quindi anche un log quasi vuoto occupa 128 MiB. A 10 M eventi: log 774 MiB + proiezione 608 MiB = **1 382 MiB, contro 608 MiB di A** (2,3×). C mantiene due copie dei dati.
- **RSS:** C 57–219 MiB contro 17–46 MiB di A. Pesa soprattutto il canale verso il proiettore (fino a 64 gruppi × 1 024 record in volo) più la cache della proiezione. È limitato, ma non è gratuito per un dispositivo Micro.

### 5.6 Soak di 30 minuti (acquisizione continua cadenzata, A e C, n = 1)

Il produttore è cadenzato a 5 000 op/s per 30 minuti: 8 osservazioni/op, 1 evento/op, 9,0 M operazioni per variante, lettore historian ogni 100 ms, checkpoint ogni 100 000 op, campioni ogni 60 s (`raw/soak-30min-5000ops.txt`). È un soak **del journal in-process**, non dell'E2E con driver.

| | A SQLite | C open-wal + proiezione |
|---|---|---|
| ritardo durevole / proiezione ai campioni | 0 / — op | 0 / 0 op (in ogni campione) |
| RSS (dal 5° al 29° minuto) | 18,8–19,1 MiB, stabile | 28,7–28,9 MiB, stabile |
| receipt p50 / p99 / p99.9 / max | 1,6 / 11,3 / 23,1 / 456 ms | 1,3 / 4,6 / 30,9 / 179 ms |
| disco a fine soak | 3 589 MiB | 7 433 MiB (2,1×) |
| CPU totale | 213 s | 273 s (+28 %) |
| lettore: p99 latenza / ritardo p99 | 4,8 ms / 64 eventi | 4,5 ms / 64 eventi |
| riavvio (apertura + checkpoint + 35 k op di replay) | 6,4 s (domina `quick_check` su 3,5 GB) | 18,3 s (scansione segmenti + riapertura proiezione) |

Al 30 % circa del punto di saturazione entrambe sono stabili per 30 minuti: nessuna crescita di memoria, nessun lag accumulato. C ha una p99 delle receipt migliore e un massimo più basso, mentre la p99.9 è peggiore. In cambio paga il doppio del disco, +28 % di CPU e un riavvio quasi 3 volte più lento.

## 6. Profilo di capacità e limiti (Micro / Server)

| | A SQLite (VS-006) | C open-wal + proiezione SQLite |
|---|---|---|
| op/s durevoli, historian pronto (1 ev/op, batch 1024, questa VM) | ~128 k | ~123 k (lag variabile, fino a decine di migliaia di eventi) |
| receipt p50 sotto saturazione | 1,3–1,8 ms | 0,9–1,25 ms |
| receipt p99.9 sotto saturazione | 8–10 ms (soak 30 min: 23 ms) | 5,6–42 ms, dispersa (soak 30 min: 31 ms) |
| soak 30 min a 5 000 op/s | stabile, 19 MiB, 3,6 GB | stabile, 29 MiB, 7,4 GB |
| disco a 10 M eventi | 608 MiB | 1 382 MiB (+128 MiB preallocati per segmento) |
| RSS sotto carico | 17–46 MiB | 57–219 MiB |
| CPU per 10 M eventi | 31 s | 40 s |
| recovery 10 M eventi | 2,8 s | 4,1 s (+34 s se la proiezione è persa) |
| disco pieno | `DISK_FULL`, retry, nessuna perdita (D13) | **FAULT, riavvio obbligatorio** |
| retention | compattazione opt-in dei payload (D13) | **nessuna** nello spike (open-wal cancella solo segmenti interi; non usata) |
| componenti | 1 file SQLite | log + checkpoint + MANIFEST + file di proiezione, due watermark |

- **Micro:** nessun miglioramento netto end-to-end, più RAM, più disco, più file e più stati. La spec §6 dice di preferire «SQLite solo»: **i dati confermano**.
- **Server:** il guadagno appare solo con commit piccoli o con molti scrittori per i quali il group commit da 1 024 non si riempie. In quel caso basterebbe anche alzare `commit_max_delay` su A (vedi celle delay). Quote CPU e affinity non sono state misurate.
- **Enterprise / PostgreSQL remoto (D):** non fatto. Il disaccoppiamento log/proiezione sarebbe la sua base naturale, ma open-wal 0.2.1 non ha un lettore concorrente: la replica oggi dovrebbe passare dal canale in memoria del writer.

## 7. Raccomandazione motivata

**Mantenere SQLite** come journal autorevole e backend predefinito. **Non promuovere open-wal e non integrarlo ora nel runtime (fase 4).** Il codice resta dietro la feature, spenta, come banco di prova riproducibile.

Motivi, in ordine di peso:

1. **Il criterio di uscita (spec §7) non è soddisfatto:** nessun miglioramento ripetibile **end-to-end A vs C** nella configurazione di riferimento (0,96×, 0,99× su 10 M eventi). I guadagni di C (1,1–1,9×) compaiono solo con commit piccoli o carichi particolari, e vengono dal disaccoppiamento della proiezione, non dal log.
2. **Costi misurati più alti:** CPU +29…+59 %, RSS 3–9×, disco 2,3× e oltre, recovery più lenta, rebuild di decine di secondi, lettori fino a 70× più indietro.
3. **Regressioni operative:** ENOSPC diventa FAULT con riavvio (oggi è un retry); il bit flip sull'ultimo record viene troncato in silenzio; nessuna retention sicura; nessun lettore concorrente nell'API 0.2.1; due watermark da gestire e spiegare.
4. **Maturità della dipendenza:** crate di 4 mesi, 3 release in 16 giorni, 174 download, un solo autore, milestone di checkpoint/retention ancora «later» nel sorgente.
5. **Un SQLite più veloce sta già nel repository:** le celle «delay» mostrano che A non migliora con il delay quando il batch è già pieno. Il collo di bottiglia con fan-out è l'inserimento SQL, che C condivide.

**Condizioni per riaprire** (lo spike può essere ripreso così com'è):

- un carico reale misurato in cui i commit restano piccoli (molti driver lenti con receipt durevoli stop-and-wait) e la latenza mediana delle receipt è il vincolo dominante;
- open-wal con un lettore concorrente (o read-only cross-process), una gestione di ENOSPC senza poison o la pre-creazione del segmento successivo, e una storia di release più lunga;
- un requisito Enterprise di replica/proiezione remota (variante D), dove il log binario separato ha un valore che SQLite integrato non dà;
- in ogni caso: integrazione runtime con T06, power loss reale su hardware Micro e campagna E2E con driver reali (§8).

## 8. Che cosa resta non provato

- **Integrazione nel runtime (fase 4) e quindi E2E con driver reali:** 1/8/16 driver × 1/128 osservazioni via IPC, ingress FIFO/source_fair, admission J4 con `worst_case_len`, T06, dedup degli ACK (T07 completo). Le forme 1–16 sorgenti × 1–128 osservazioni sono state misurate **in-process**, senza socket.
- **Power loss reale e hardware ARM64/Micro** (T12): solo compile check aarch64. Nessuna misura su Raspberry/eMMC/SD.
- **PostgreSQL** (variante C Enterprise, variante D) e quote CPU/affinity.
- **Soak:** solo journal in-process (30 min, n = 1 per variante), non E2E; nessuna prova DISK_FULL durante il soak (ENOSPC è testato in T04).
- **Milioni di operazioni con consumer arretrato oltre il bound del canale:** il design attuale applica backpressure al writer, non lascia accumulare lag oltre il canale.
- Gli **advisories** sono verificati solo come indicato in §2.

