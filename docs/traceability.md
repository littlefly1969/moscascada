# Matrice di tracciabilità VS-001

Legenda: **✅ verificato** = implementato e coperto da un test o da una misura eseguiti (risultati in questo repository); **🟡 parziale** = implementato, verifica incompleta (motivo indicato); **❌ non soddisfatto** = non implementato o non verificabile qui (motivo indicato).

Percorsi dei test: `core/` = `crates/scada-core/tests/`, `core:kernel` = test unitari in `crates/scada-core/src/kernel.rs`, `proto/` = `crates/scada-protocol/tests/`, `rt/` = `crates/scada-runtime/tests/`.

Esecuzione di riferimento (30-10-2026, Linux x86_64): `cargo test --workspace` → 60 test, 0 falliti (log: `docs/benchmarks/results/check-20261030.log`); `./scripts/check.sh` → tutte le verifiche superate (vedi "Evidenze" in fondo).

## §1 Vincoli architetturali

| # | Requisito | Stato | Evidenza |
|---|---|---|---|
| 1.1 | Rust stable, Linux-first, `Cargo.lock`, toolchain documentata | ✅ | `rust-toolchain.toml` (1.97.0), `Cargo.lock` committato, README |
| 1.1 | Compatibilità di compilazione ARM64 | 🟡 | `cargo check --workspace --all-targets --target aarch64-unknown-linux-gnu` passa (in `scripts/check.sh`). Il **link** non è stato provato: nell'ambiente manca un sysroot glibc aarch64 (`ld.lld: cannot open Scrt1.o`). |
| 1.1 | Esecuzione reale su Raspberry Pi / ARM64 | ❌ | **not tested**: nessun hardware ARM64 disponibile. |
| 1.2 | Kernel e driver processi differenti; core senza MQTT/JSON/TCP/libreria driver | ✅ | `scada-kernel` e `scada-sim-driver` sono binari distinti; i test `rt/ipc.rs` lanciano processi reali. `check.sh` verifica con `cargo tree` che `scada-core` non dipenda da tokio/prost/serde/toml. |
| 1.3 | State machine pura, sincrona, single writer, ingressi espliciti e ordinati | ✅ | `Kernel::apply(&mut self, SequencedInput)`; audit di purezza in `check.sh` (nessun `SystemTime`, `Instant`, `std::net`, `std::thread`, `std::fs`, tokio in `scada-core/src`); unico proprietario: thread `scada-sequencer`. |
| 1.4 | RTDB solo in RAM, non durabilità esplicita | ✅ | Nessun I/O su disco nel kernel; `ACK_STATUS_APPLIED_VOLATILE` documentato in `driver.proto` e `contract.md` §3; `rt/ipc.rs::kernel_restart_loses_volatile_state_explicitly`. |
| 1.5 | Strutture a memoria limitata | ✅ | Limiti configurati per connessioni, frame, ACK in volo, coda sequencer, batch, canali/sorgente, dedup (1 batch/sorgente). `core:kernel::apply_never_grows_internal_storage`; benchmark core: 0 allocazioni per `apply` e Δ heap 0 su 1M operazioni; saturazione 60 s con RSS stabile. |
| 1.6 | All-or-nothing per `ObservationBatch` | ✅ | `core/transitions.rs::batch_with_unknown_channel_changes_nothing`, `other_validation_errors_change_nothing`; `core/determinism.rs::prop_one_bad_observation_rejects_whole_batch`; invariante I1 controllata a ogni passo di 80 000 input casuali; fuzz `batch_validation` (12,3 M esecuzioni). |
| 1.7 | Determinismo dato l'ordine sequenziato | ✅ | `core/determinism.rs::seeded_random_streams_are_deterministic` (200 seed × 400 input), `prop_two_kernels_same_stream` (256 casi), `replaying_the_same_stream_gives_the_same_final_digest`. |
| 1.8 | Nessun `unsafe` applicativo; niente panic su input non fidato | ✅ | `forbid(unsafe_code)` nei crate di prodotto; unica eccezione nel benchmark (ADR 0003), verificata da `check.sh`. Core e protocol negano `unwrap/expect/panic/indexing/arithmetic_side_effects` fuori dai test. Fuzz: 0 crash in 20,9 M esecuzioni; storm di 2000 connessioni con byte casuali (`rt/ipc.rs::malformed_frames_do_not_crash_or_allocate`). |
| 1.9 | Driver restart indipendente, kernel vivo, qualità degradata, ultimo valore preservato | ✅ | `rt/ipc.rs::kernel_survives_driver_kill` (SIGKILL del processo driver). |

## §2 Ambito funzionale

| Requisito | Stato | Evidenza |
|---|---|---|
| Crate `scada-core`, `scada-protocol`, `scada-runtime`, binario `scada-sim-driver` | ✅ | Workspace |
| `.proto` versionato + tool di verifica schemi | ✅ | `proto/scada/driver/v1/driver.proto`, `scripts/check-proto.sh` (buf lint, buf breaking vs baseline, prova di rilevamento) |
| Config statica validata all'avvio (sorgenti, binding, tipi, limiti) | ✅ | `core/config_validation.rs` (11 test), `rt/config_file.rs` (processo che esce con codice 3) |
| Tipi `bool` e `i64`; temperatura come intero scalato | ✅ | `ValueType::{Bool, I64}`; tag 103 `case_temperature_centi_c`. Nessun terzo tipo: non giustificato da un requisito della slice. |
| Valore, qualità, revisione; iniziale `Unknown`; `Good`/`Uncertain(SourceUnavailable)`/`Unknown` | ✅ | `core/transitions.rs::initial_state_is_unknown`, `value_quality_revision_and_identical_reacquisition` |
| Handshake versione + identità + autorizzazione locale | ✅ | `rt/ipc.rs::handshake_rejections_are_explicit` |
| `FullState` / `Delta`, ACK espliciti | ✅ | `rt/ipc.rs::demo_scenario_with_separate_driver_process` |
| `kernel_instance_id` nuovo a ogni avvio, nel `Welcome` | ✅ | `rt/ipc.rs::kernel_restart_loses_volatile_state_explicitly` |
| Ordine globale degli ingressi ammessi, un solo writer | ✅ | thread sequencer + coda FIFO; `rt/ipc.rs::backpressure_when_queue_is_full` (operation_sequence unici e contigui) |
| Introspezione read-only: snapshot logico e digest | ✅ | `Kernel::logical_snapshot`, `Kernel::state_digest`; socket `scada.introspect.v1` usato da tutti i test multiprocesso |
| Disconnessione/riavvio/reconnect del driver con test | ✅ | `kernel_survives_driver_kill`, `demo_scenario…`, `superseded_link…`, `concurrent_reconnects…` |
| Test unitari, integrazione, property-based, fuzz, benchmark | ✅ | vedi §7 e §8 |
| Esclusioni (MQTT, Modbus, allarmi, historian, DB, UI, plugin, Raft…) | ✅ | Nessuna di queste funzionalità o dipendenze è presente (`cargo tree`). |

## §3 IPC

| Requisito | Stato | Evidenza |
|---|---|---|
| ADR 0001 con confronto Protobuf / Cap'n Proto / gRPC / shared memory (+ FlatBuffers) | ✅ | `docs/adr/0001-ipc.md` con fonti primarie; nessuna conclusione quantitativa non misurata |
| UDS stream + prost, framing `u32` BE + payload, connessione persistente | ✅ | `scada-protocol::frame`, `proto/frame.rs::wire_format_anchor` |
| Tokio solo ai bordi I/O | ✅ | Tokio solo in `scada-runtime`; il core gira su un thread OS senza runtime |
| Limiti configurabili con default 65536/128/16/64 | ✅ | `config/demo.toml`; `rt/config_file.rs::shipped_demo_config_is_valid` |
| Lunghezza invalida rifiutata prima di allocare | ✅ | `proto/frame.rs::oversize_length_rejected_before_allocation` (capacità buffer = 0); `rt/ipc.rs::malformed_frames…` (RSS stabile con lunghezze fino a 4 GiB) |
| Sorgenti con più canali del limite `FullState` rifiutate in config | ✅ | `core/config_validation.rs::source_with_more_channels_than_a_full_state_can_carry` |
| Timeout handshake e frame incompleti | ✅ | `handshake_rejections_are_explicit` (HANDSHAKE_TIMEOUT), `malformed_frames…` (FRAME_TIMEOUT su header e payload troncati) |
| Backpressure senza drop silenziosi | ✅ | `rt/ipc.rs::backpressure_when_queue_is_full`; benchmark di saturazione |
| Directory/socket con permessi ristretti | 🟡 | Socket `0660`, directory creata `0750`, introspezione `0600`. Il cambio di gruppo (`socket_group_gid`) è implementato ma **non testato** (richiede un secondo gruppo nell'ambiente). Finestra fra `bind` e `chmod` mitigata solo dai permessi della directory (documentato in ADR 0001). |
| Credenziali del peer Unix e autorizzazione `source_id` | ✅ | `SO_PEERCRED` via `UnixStream::peer_cred`; `authorized_uids` per sorgente; test positivo (uid corrente) e negativo (UNAUTHORIZED). |
| Connessioni malevole senza crash, risorse limitate | ✅ | `malformed_frames…` (2000 connessioni con byte casuali), `connection_limit_is_enforced_with_explicit_refusal` |
| Namespace `scada.driver.v1`, `oneof`, codici errore espliciti, niente `map` | ✅ | `driver.proto`; `buf lint` STANDARD |
| `buf lint` e `buf breaking` | 🟡 | Entrambi eseguiti e superati. Come richiesto dal brief, `buf breaking` contro la baseline v1.0 **non è ancora significativo** (baseline = schema attuale); lo script dimostra comunque che una rinumerazione di campo viene rilevata. |

## §4 Contratto semantico

| Requisito | Stato | Evidenza |
|---|---|---|
| Identità (`driver_id`, `source_id`, `source_epoch`, `link_generation`, `source_sequence`, `operation_sequence`, `kernel_instance_id`) e relazione driver/sorgente | ✅ | `docs/contract.md` §1, commenti in `driver.proto` |
| `Hello` / `Welcome` / errore esplicito / `ObservationBatch` / `InputAck` | ✅ | `driver.proto`; test IPC |
| Un input alla volta; ordine fra driver scelto dal sequencer | ✅ | thread sequencer; documentato (non si promette lo stesso ordine a ricevitori indipendenti) |
| Canale duplicato nel batch ⇒ batch invalido | ✅ | `other_validation_errors_change_nothing` (DuplicateChannel) |
| Canonizzazione per `tag_id`, niente `HashMap` | ✅ | `prop_observation_order_irrelevant`; nessuna `HashMap` in `scada-core` |
| Solo sessione/epoca corrente per sorgente; `Delta` contiguo; gap ⇒ `SequenceGap`; overflow esplicito | ✅ | `gap_stale_type_and_resync`, `core:kernel::source_sequence_overflow_is_explicit`, `new_epoch_requires_full_state` |
| `Duplicate` senza riapplicare; `SequenceConflict`; `StaleSequence`; ultimo payload conservato | ✅ | `identical_duplicate_of_last_batch`, `same_sequence_different_payload_is_a_conflict`. Si conserva la copia canonica dell'ultimo batch (superinsieme dell'hash, ADR 0002). |
| Primo `FullState` dell'epoca con sequenza >1, continuità non dimostrata segnalata | ✅ | `first_full_state_may_start_at_any_positive_sequence`; `CONTINUITY_BASELINE_ESTABLISHED` / `RESYNCHRONIZED` |
| `FullState` completo o rifiuto | ✅ | `other_validation_errors_change_nothing` (IncompleteFullState) |
| Reconnect senza perdita documentato | ✅ | `contract.md` §5 "Reconnect senza perdita"; demo: ritrasmissione ⇒ `DUPLICATE`, poi sequenza successiva |
| Nuovo kernel: tutto `Unknown`, nuova negoziazione e `FullState` | ✅ | `kernel_restart_loses_volatile_state_explicitly` |
| `SourceConnected`/`SourceDisconnected` come input ordinati | ✅ | `Operation::{SourceConnected, SourceDisconnected}` passano dal sequencer |
| Disconnessione link attivo ⇒ `Uncertain`, valore preservato; disconnessione vecchia ignorata | ✅ | `disconnect_preserves_value_reconnect_stays_uncertain`, `late_disconnect_of_old_generation_is_ignored` |
| Riconnessione non rimette `Good` | ✅ | idem + `kernel_survives_driver_kill` |
| `fault=true` è dato `Good` | ✅ | `value_quality_revision_and_identical_reacquisition` (tamper=true ⇒ Good) |
| Nessun timeout nel core | ✅ | audit di purezza |
| `AppliedVolatile` solo dopo il ritorno di `apply`; nessun `DurableCommitted` | ✅ | `sequencer.rs` (reply dopo `apply`); `backpressure_when_queue_is_full` (ogni ACK ha un `operation_sequence` unico applicato); nessuno stato durevole nello schema |
| Health/diagnostica | ❌ (opzionale) | Non implementato: non richiesto; l'introspezione copre le esigenze di test. |

## §5 Modello formale del core

| Requisito | Stato | Evidenza |
|---|---|---|
| `apply`, `logical_snapshot`, `state_digest` | ✅ | `crates/scada-core/src/kernel.rs` |
| `operation_sequence == prev + 1`; rifiuto non consuma | ✅ | `out_of_order_operation_sequence_is_rejected`, `rejected_and_duplicate_inputs_do_not_consume_operation_sequence`, invariante I3 |
| Ordine di ammissione vs transizioni applicate formalizzati | ✅ | ADR 0002 §2 |
| Storage indicizzato, metadati immutabili separati dalle celle | ✅ | `ValidatedConfig` (in `Arc`) + `Vec<TagCell>` |
| Validazione completa prima di mutare; `S' == S` su errore; overflow espliciti | ✅ | invariante I1 in tutti i test; `core:kernel::revision_overflow_is_rejected_without_mutation`, `operation_sequence_exhaustion_is_explicit` |
| Revisione solo su cambio `(value, quality)`; nessun wrap | ✅ | invariante I4; `revision_overflow_…` |
| Digest canonico versionato, BLAKE3, stato dedup incluso, esclusioni | ✅ | ADR 0002 §7; `digest_v1_golden_value`; `digest_excludes_nothing_logical`; calcolato solo su richiesta |

## §6 Demo

| Requisito | Stato | Evidenza |
|---|---|---|
| Sorgente 1001, canali 1-3 → tag 101-103 | ✅ | `config/demo.toml` |
| FullState, Delta, duplicato, gap, batch invalido, disconnessione, reconnect, automatizzati | ✅ | `rt/ipc.rs::demo_scenario_with_separate_driver_process` |

## §7 Definition of Done

| # | Test | Stato | Test |
|---|---|---|---|
| 1 | Config errata ⇒ avvio rifiutato | ✅ | `core/config_validation.rs`, `rt/config_file.rs::kernel_binary_exits_with_error_on_invalid_config` |
| 2 | Valore, revisione, qualità; valore identico non altera revisione | ✅ | `value_quality_revision_and_identical_reacquisition` |
| 3 | 2 validi + canale sconosciuto ⇒ nessuna modifica, stessa sequenza, stesso digest | ✅ | `batch_with_unknown_channel_changes_nothing` |
| 4 | Duplicato ⇒ `Duplicate`, stesso digest e sequenza | ✅ | `identical_duplicate_of_last_batch` |
| 5 | Stessa sequenza, payload diverso ⇒ `SequenceConflict` | ✅ | `same_sequence_different_payload_is_a_conflict` |
| 6 | Gap, vecchia, overflow, tipo errato ⇒ errore tipizzato, stato invariato | ✅ | `gap_stale_type_and_resync`, `core:kernel::source_sequence_overflow_is_explicit` |
| 7 | Disconnessione/reconnect e qualità | ✅ | `disconnect_preserves_value_reconnect_stays_uncertain` |
| 8 | Disconnessione di generazione superata ignorata | ✅ | `late_disconnect_of_old_generation_is_ignored` |
| 9 | Due core, stessa sequenza (anche casuale con seed) ⇒ snapshot e digest identici a ogni passo | ✅ | `seeded_random_streams_are_deterministic`, `prop_two_kernels_same_stream` |
| 10 | `operation_sequence` fuori ordine rifiutato | ✅ | `out_of_order_operation_sequence_is_rejected` |
| 11 | Processi reali su UDS; kernel sopravvive al kill del driver | ✅ | `kernel_survives_driver_kill` |
| 12 | Frame oversize, zero, corrotto, troncato senza crash né allocazioni arbitrarie | ✅ | `malformed_frames_do_not_crash_or_allocate`, `proto/frame.rs` |
| 13 | Versione incompatibile o sorgente non autorizzata ⇒ rifiuto | ✅ | `handshake_rejections_are_explicit` |
| 14 | Coda piena ⇒ backpressure, niente crescita, drop o ACK anticipati | ✅ | `backpressure_when_queue_is_full` |
| 15 | Restart kernel ⇒ nuovo id, `Unknown`, `FullState`, nessun falso recovery | ✅ | `kernel_restart_loses_volatile_state_explicitly` |
| 16 | Disconnessioni/riconnessioni concorrenti non corrompono lo stato | ✅ | `superseded_link_cannot_overwrite_new_generation`, `concurrent_reconnects_do_not_corrupt_state` |
| F1 | Fuzz decoder/framing e validazione batch, corpus iniziale valido/malformato | ✅ | `fuzz/fuzz_targets/*`, `fuzz/seeds/` (15 seed). Esecuzione locale 300 s per target: 0 crash (vedi sotto). |
| F2 | Separazione nightly/stable documentata | ✅ | `fuzz/Cargo.toml` (workspace separato), README, job CI `fuzz-smoke` |
| T1 | `cargo fmt --check`, `clippy -D warnings`, `cargo test`, `buf lint`, verifica schemi | ✅ | `scripts/check.sh` |
| T2 | Versioni effettive degli strumenti | ✅ | README "Toolchain" e `docs/benchmarks/vs001.md` |

## §8 Benchmark

| Requisito | Stato | Evidenza |
|---|---|---|
| Kernel: transizioni/s, p50/p99/p99.9, RAM, allocazioni/op | ✅ | `docs/benchmarks/vs001.md` §1 |
| E2E: oss/s, batch/s, latenza send→ACK per 1/32/128 oss, 1 e 8 client | ✅ | §2 |
| Memoria e risorse: RSS idle/regime/picco, CPU, dimensione binari, thread, dipendenze | ✅ | §2, §4 |
| Allocazioni per operazione nel processo kernel end-to-end | ❌ | Non misurate: l'allocatore di conteggio è installato solo nel processo di benchmark (core in-memory). Strumentare il kernel richiederebbe `unsafe` nel binario di prodotto. |
| Saturazione: produttore più veloce, backpressure, memoria limitata sotto carico prolungato | ✅ | §3 (60 s) |
| Hardware: Linux di sviluppo; ARM64 se disponibile | 🟡 | x86_64 VM misurata; **ARM64: not tested** |
| Metodologia (hardware, OS, release, compilatore, tag, warm-up, durata) | ✅ | §0 |

## §11 Criteri di rifiuto (auto-verifica)

| Criterio | Esito |
|---|---|
| Kernel carica codice driver / ricompilazione per il simulatore | No: processi e binari separati, solo schema condiviso |
| Core con Tokio, socket o `SystemTime::now()` | No: audit `check.sh` |
| Validazione fallita con modifiche parziali | No: invariante I1 + fuzz |
| ACK che implica durabilità | No: `APPLIED_VOLATILE`, nessuno stato durevole |
| Stato/digest dipendenti da mappe o identità di processo | No: nessuna `HashMap`, `kernel_instance_id` escluso dal digest |
| Code/frame/container illimitati | No: ogni struttura ha limite (§1.5) |
| Crash del driver ⇒ crash del core | No: `kernel_survives_driver_kill` |
| Test IPC solo mock in-process | No: processi reali |
| Benchmark senza metodologia o numeri non misurati | No: `vs001.md` |
| Complessità non giustificata | Nessun framework, plugin, DB, Raft o DSL |

## Evidenze

- Gate completo: `./scripts/check.sh` → `ALL CHECKS PASSED` (log in `docs/benchmarks/results/check-20261030.log`): fmt, clippy `-D warnings`, 60 test, audit `unsafe`, audit dipendenze e purezza del core, `buf lint`, `buf breaking` + prova di rilevamento, `cargo check` aarch64.
- Fuzz: `docs/benchmarks/results/fuzz-20261030.md`.
- Benchmark: `docs/benchmarks/vs001.md`, output grezzo `docs/benchmarks/results/vm-20261030.md`.
- Stabilità dei test multiprocesso: la suite `rt/ipc.rs` è stata eseguita 8 volte consecutive senza fallimenti dopo la correzione di due race nei test stessi (vedi sotto).

## Limiti noti

1. **ARM64**: solo `cargo check`; link ed esecuzione non testati (manca sysroot/hardware).
2. **Durabilità assente per progetto**: nessun journal, nessun recovery. Un riavvio perde tutto; il protocollo lo rende esplicito (`kernel_instance_id`, `APPLIED_VOLATILE`, `full_state_required`).
3. **Ordine di ammissione non registrato**: il sequencer non scrive un log degli input; la riproducibilità è garantita dal core dato lo stesso stream (testata), ma lo stream reale non viene salvato (sarebbe un journal, fuori perimetro).
4. **Takeover**: un link superato ma inattivo resta aperto finché il peer non scrive o chiude, occupando uno slot di connessione (limite `max_driver_connections`). Solo un peer già autorizzato può farlo.
5. **Rifiuto per limite connessioni best effort**: se il client ha già inviato dati, Linux può consegnargli `ECONNRESET` invece del frame `TOO_MANY_CONNECTIONS`.
6. **Permessi del socket**: piccola finestra fra `bind` e `chmod 0660` (mitigata dalla directory `0750` se creata dal kernel); `socket_group_gid` non testato.
7. **Autorizzazione**: per UID effettivo e solo se `authorized_uids` è configurato; altrimenti valgono i soli permessi del file socket. Nessun controllo per GID/PID.
8. **Flag `--throttle-sequencer-us`** presente nel binario di produzione per i test di saturazione (default 0, documentato come diagnostico).
9. **Introspezione**: socket opzionale che restituisce l'intero snapshot (allocazione proporzionale al numero di tag, fuori dal percorso caldo); non è un'API pubblica.
10. **Benchmark**: una sola esecuzione per cella su una VM condivisa; nessun intervallo di confidenza; massimi rumorosi.
11. **Allocazioni nel kernel end-to-end non misurate** (solo nel core in-memory).
12. **Digest lento** (83 µs per 1024 tag) per BLAKE3 in Rust puro e molti `update` piccoli; accettabile perché on-demand.

## Problemi trovati durante lo sviluppo (e corretti)

- Messaggi Protobuf con tutti i campi di default si codificano in 0 byte, ma un frame di lunghezza 0 è invalido: `IntrospectRequest{include_tags=false}` non era inviabile. Corretto aggiungendo `introspect_version` (= 1) allo schema prima della baseline. `DriverMessage`/`KernelMessage` non hanno il problema (il `oneof` è sempre presente).
- Due race nei test multiprocesso (non nel kernel): attesa delle connessioni ancora in backlog prima di verificare `active_connections == 0`, e lettura del rifiuto `TOO_MANY_CONNECTIONS` dopo che il client aveva già scritto (ECONNRESET, limite 5).
- Il contatore di sequenza sorgente a `u64::MAX` dava `StaleSequence`; ora restituisce `SequenceOverflow` esplicito (serve una nuova epoca).
- Il sequencer che termina in modo anomalo lasciava il processo vivo ma inutile: ora il kernel esce con errore.

## Valutazione critica delle scelte

- **State machine pura + sequencer su thread dedicato**: ha reso semplice dimostrare determinismo e atomicità (proprietà verificate su ~80 000 input casuali e 12 M input fuzz senza violazioni). Il costo misurato è altrove: il throughput end-to-end resta a ~40 000 batch/s indipendentemente dalla dimensione del batch, mentre `apply` da 128 osservazioni costa 3,3 µs (~250 000/s). Il passaggio Tokio → thread → `oneshot` → Tokio per ogni richiesta domina. Alternative da valutare **con misure**: drenare più richieste per risveglio del sequencer, oppure eseguire `apply` direttamente nel loop `current_thread` (resta single-writer ma perde l'isolamento dal runtime).
- **Copia canonica dell'ultimo batch invece dell'hash**: confronto esatto e zero hashing sul percorso caldo; costo 16 byte × canali per sorgente. Scelta conservativa, coerente con "memoria limitata".
- **prost + doppia conversione** (proto → `Vec<Observation>` core): due allocazioni per batch nel runtime; misurabile solo strumentando il kernel. Il core resta a 0 allocazioni.
- **Takeover vs rifiuto della seconda connessione**: il takeover semplifica il riavvio rapido del driver e rende testabile la regola "il vecchio link non sovrascrive", al prezzo del limite noto 4.
- **Rust puro ovunque (protox, blake3 `pure`)**: build senza toolchain C o `protoc`, utile per ARM64; costo visibile solo nel digest.
- **Introspezione su socket separato**: permette test multiprocesso reali senza mock e senza introdurre un'API pubblica; è un'area da tenere disattivata in esercizio.

## Prossimo rischio prioritario

Il costo per richiesta del runtime (handoff fra thread) limita il throughput a ~40 000 batch/s su questa VM e la latenza p50 a ~100 µs anche quando la state machine lavora per meno di 4 µs. Prima di aggiungere funzionalità (allarmi, historian) conviene misurare e ridurre questo percorso, ripetendo i benchmark su hardware ARM64 reale, che oggi è il dato mancante più importante per la piattaforma di destinazione.
