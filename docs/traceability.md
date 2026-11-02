# Matrice di tracciabilità VS-001 + VS-002 + VS-003 + VS-004 + VS-005 + VS-006

La parte VS-002 è in fondo ([§VS-002](#vs-002)), seguita da [§VS-003](#vs-003), [§VS-004](#vs-004), [§VS-005](#vs-005) e [§VS-006](#vs-006); le sezioni VS-001 sono conservate, con i limiti noti aggiornati dove VS-002 li ha chiusi.

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
4. ~~**Takeover**: un link superato ma inattivo resta aperto…~~ **Chiuso in VS-002 (D3)**: il sequencer segnala il link superato, che viene chiuso (`LINK_SUPERSEDED`); test `vs002.rs::repeated_takeovers_do_not_exhaust_connection_slots`.
5. **Rifiuto per limite connessioni best effort**: se il client ha già inviato dati, Linux può consegnargli `ECONNRESET` invece del frame `TOO_MANY_CONNECTIONS`.
6. **Permessi del socket**: piccola finestra fra `bind` e `chmod 0660`; VS-002 (E2) richiede ora una directory non scrivibile da altri, non symlink, del proprio utente o di root, e non rimuove socket altrui. `socket_group_gid` resta non testato.
7. **Autorizzazione**: per UID effettivo e solo se `authorized_uids` è configurato; altrimenti valgono i soli permessi del file socket. Nessun controllo per GID/PID.
8. **Flag `--throttle-sequencer-us`** presente nel binario di produzione per i test di saturazione (default 0, documentato come diagnostico).
9. **Introspezione**: socket opzionale che restituisce l'intero snapshot (allocazione proporzionale al numero di tag, fuori dal percorso caldo); non è un'API pubblica.
10. **Benchmark**: in VS-001 una sola esecuzione per cella. VS-002 usa 7 run randomizzate per condizione con dispersione e test di permutazione (`docs/benchmarks/vs002.md`); resta una sola VM.
11. ~~**Allocazioni nel kernel end-to-end non misurate**~~ **Misurate in VS-002** con `valgrind --tool=dhat`: 9,0 → 2,0 allocazioni per batch da 128 oss. (`vs002.md` §6).
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


---

## VS-002

Brief: `MoscaSCADA_VS002_Agent_Brief.md`. Base `b17413f` (VS-001), branch `vs-002`. Esecuzione di riferimento (31-10-2026, VM x86_64 4 vCPU): `cargo test --workspace --release` e `SCADA_TEST_SCHEDULER=multi_thread:2 cargo test --release -p scada-runtime` verdi; `cargo fmt --check`, `clippy -D warnings`, `buf lint`/`buf breaking` superati; fuzz 3 target × 300 s, 0 crash.

### Vincoli (§0, "non fare")

| Vincolo | Stato | Evidenza |
|---|---|---|
| `scada.driver.v1` invariato | ✅ | Nessuna modifica a `driver.proto`; `buf breaking` contro la baseline v1.0. Le aggiunte sono solo in `introspect.proto` (campi additivi). |
| Semantica `APPLIED_VOLATILE`, `Kernel::apply` puro, un solo writer, modello Bool/I64, UDS/Protobuf | ✅ | core: solo indice denso (stesso risultato, `core/equivalence.rs` contro il kernel VS-001 congelato); `check.sh` audit di purezza esteso (lock, `unsafe`) |
| Niente I/O, timer, lock, thread nel core; niente `unsafe` nel prodotto | ✅ | `check.sh` |
| Niente cluster, DB, historian, allarmi, logica, GUI, MQTT | ✅ | `cargo tree`; la sorgente "critica" è solo un ruolo nel benchmark |
| Niente spin loop, affinità, `SCHED_FIFO`, lock-free custom, allocatore custom, mmap, compressione | ✅ | Non presenti |
| Nessuna coda illimitata, nessuna perdita/coalescenza silenziosa, nessun riordino intra-sorgente | ✅ | Tutte le code limitate; il coalescing riguarda solo le `write` di ACK già prodotti, in ordine; test di ordine §8.3/§8.4 |
| Nessuna dichiarazione hard real-time / ARM64 | ✅ | `vs002.md` §11, README: **ARM64 NOT TESTED ON HARDWARE** |

### Work package

| WP | Requisito | Stato | Evidenza |
|---|---|---|---|
| A | Strumentazione non intrusiva prima degli interventi | ✅ | `diag.rs` (istogrammi limitati, `--diag`, `--diag-probe-source`), CPU per thread nel bench; overhead entro il rumore (`vs002.md` §2) |
| B | Scheduler `current_thread`/`multi_thread(N)` esplicito, sequencer dedicato, ordine | ✅ | `config.rs` (`[runtime]`), ADR 0004, campagna 2, test di ordine con 1/2/4 worker |
| C1 | Drain del sequencer | ✅ misurato, ❌ non adottato | ADR 0005 (codice rimosso dopo la misura) |
| C2 | ACK senza allocazioni ripetute | ✅ | `server.rs::set_ack`, test `reused_ack_encodes_identically`, dhat |
| C3 | Decode/conversione senza allocazioni ripetute | ✅ | `convert.rs::decode_driver_message`, proptest + fuzz `decode_equivalence` |
| P1 (review) | Memoria del decoder entro il budget: nessuna capacità trattenuta oltre `2·max`, picco transitorio contato per worker | ✅ | ADR 0006 §P1 (varianti A0/A/B misurate, B adottata), ADR 0009, `adversarial_many_small_observations_are_rejected_without_retained_memory` (`ct` e `mt2`), `convert.rs::adversarial`, proptest con limite, fuzz `results/vs002/fuzz/p1/`, `vs002.md` §12 |
| C4 | Lookup/sort del core | ✅ | ADR 0007 |
| C5 | Syscall per batch (buffer lettura, ACK raggruppati) | ✅ | ADR 0006, campagna 1 |
| D1 | Ripresa con fotografia fresca | ✅ | `scada_sim_driver::resume`, contratto §5.1, `reconnection_with_partial_deltas_then_fresh_full_state`, `simulator_resume_procedure_reasserts_all_channels` |
| D2 | Scadenza scrittura ACK / peer non leggente | ✅ | `ack_write_timeout_ms`, `non_reading_peer_is_closed_and_reconciles_by_reconnect` |
| D3 | Takeover: chiusura del vecchio link | ✅ | `repeated_takeovers_do_not_exhaust_connection_slots` |
| P2 (review) | Takeover di un link in backpressure che non legge: slot liberato subito | ✅ | ADR 0008 §P2, contratto §2.1, `takeover_of_a_backpressured_non_reading_link_releases_its_slot_promptly` (`current_thread` e `multi_thread(2)`) |
| D4 | Ordine su runtime multicore | ✅ | `ingestion_modes_preserve_order_and_give_the_same_normalized_state`, suite runtime con `multi_thread:2` |
| D5 | Replay deterministico dello stream sequenziato | ✅ | `capture.rs`, `captured_streams_replay_to_the_live_digest_in_every_mode` (replay su `Kernel` e sul kernel VS-001 congelato) |
| D6 | Nessun timeout del dispositivo nel core | ✅ | Nessuna modifica a `apply` |
| E1 | Bound configurativo `u128`, `memory_budget_bytes` | ✅ | `budget.rs` + test, ADR 0009, `--check-config` |
| E2 | Directory/socket, symlink, cleanup, socket altrui | ✅ / 🟡 | `tests/socket_security.rs` (7 test). Il caso "socket stale di un altro utente" è eseguito solo come root (qui sì). Finestra `bind`→`chmod` documentata. |
| E3 | Introspezione limitata, fuzz e malformati sui percorsi modificati, `max_frame_bytes` prima dell'allocazione | ✅ | idle timeout introspezione, `malformed_input_with_read_buffer` / `_unbuffered`, fuzz |
| E4 | `cargo audit`, `cargo deny` | ✅ | `docs/benchmarks/results/vs002/supply-chain/` (0 advisory; licenze ok; 2 duplicati di versione segnalati come warning: `hashbrown`, `syn`) |

### Test obbligatori (§8)

| # | Test | Stato | Evidenza |
|---|---|---|---|
| 1 | Stesso stream → stesso snapshot/digest prima/dopo e con ogni worker count | ✅ | `core/equivalence.rs` (200 seed × 400 passi + proptest contro VS-001), `captured_streams_replay_to_the_live_digest_in_every_mode` |
| 2 | Invalidi all-or-nothing, duplicati, gap, stale invariati | ✅ | Suite VS-001 invariata e verde; equivalenza differenziale |
| 3 | Ordine ACK con 8/16 sorgenti, ricadute e takeover | ✅ | `ingestion_modes_preserve_order…` (16 sorgenti, takeover) |
| 4 | `current_thread` e `multi_thread(1/2/4)`, stesso scenario normalizzato | ✅ | idem; CI con `multi_thread(2)` |
| 5 | Client che non legge ACK | ✅ | `non_reading_peer_is_closed_and_reconciles_by_reconnect` |
| 6 | Frame incompleti, oversize, malformati, EOF | ✅ | `malformed_input_*`, `rt/ipc.rs::malformed_frames…`, fuzz |
| 7 | Reconnect con delta parziali → `Uncertain`; `FullState` fresco → `Good` | ✅ | `reconnection_with_partial_deltas_then_fresh_full_state` |
| 8 | Replay continuo vs perdita di sequenza | ✅ | `continuous_replay_vs_sequence_loss` |
| 9 | Sorgente rara vs alto volume, p50/p99/p99.9, niente fame/perdita | ✅ | campagna 3, `vs002.md` §5 (con i limiti statistici dichiarati) |
| 10 | Memoria/limiti ≥ 60 s, RSS stabile, budget senza overflow | ✅ | `vs002.md` §9, `budget::tests` |
| 11 | Terminazione del sequencer → arresto dell'host | ✅ | `sequencer_failure_stops_the_host` |
| 12 | fmt, clippy, test, buf, fuzz, Miri | 🟡 | Tutti eseguiti tranne **Miri**: non eseguito (i moduli toccati sono codice safe senza `unsafe`; il valore di Miri qui sarebbe basso rispetto al costo). |

### Definition of Done (§12)

| Voce | Stato | Evidenza |
|---|---|---|
| Branch e PR dedicata senza merge, SHA base/finale, changelog | ✅ | PR verso `vs-001`; SHA nella descrizione |
| Build release e test verdi; fuzz ripetuto sui componenti modificati | ✅ | sopra |
| `vs002.md`, log grezzi, analisi colli di bottiglia | ✅ | `docs/benchmarks/vs002.md`, `docs/benchmarks/results/vs002/` |
| architecture, contract, traceability, ADR | ✅ | ADR 0004–0009 |
| Tabella baseline/variante/risultato/variazione/regressioni/decisione con scarti | ✅ | `vs002.md` §1 |
| Reconnect con snapshot fresco, non-reading client, limiti di memoria | ✅ | D1, D2, E1 |
| Profili Micro e Server | ✅ | `config/profiles/`, test `shipped_profiles_are_valid_and_fit_their_budget`, demo nel README |
| Limiti residui | ✅ | sotto |

### Limiti e rischi residui (VS-002)

1. **Nessuna durabilità**: stato solo in RAM, `APPLIED_VOLATILE`; la cattura `--capture-inputs` è diagnostica, non un journal.
2. **Nessuna garanzia hard real-time**: latenze proporzionali al carico e alla coda; nessun isolamento fra sorgenti (la sorgente rara attende dietro le altre nella FIFO). Fairness per sorgente non implementata (proposta in `vs002.md` §5).
3. **Nessuna ridondanza**: un processo, un sequencer.
4. **ARM64: NOT TESTED ON HARDWARE** (né link né esecuzione).
5. Misure su una sola VM con client co-residenti; `worker_threads = 2` è un valore di questa VM.
6. Il thread sequencer è al 64 % con `mt2` a 125 kbatch/s: con più core per l'I/O diventerà il collo di bottiglia (single writer per progetto).
7. Finestra `bind`→`chmod` del socket; controllo solo della directory immediata, non dei componenti superiori del percorso.
8. Bound di memoria = allocazioni applicative con una stima fissa per task Tokio; non è un bound dell'RSS.
9. D3 cambia un comportamento osservabile (il link superato viene chiuso subito, anche in backpressure, e i suoi ACK non ancora scritti possono andare persi); documentato nel contratto §2.1.
10. Miri non eseguito.
11. **Fairness per sorgente**: non implementata; serve un ADR dedicato (code per sorgente con round robin nel sequencer vs. quote per sorgente) prima di affrontarla. Follow-up indicato dalla review.
12. **Raspberry Pi / ARM64 reale**: nessun test su hardware; da eseguire (build nativa, suite, campagna 3 ridotta, soak). Follow-up indicato dalla review.
13. **Limiti funzionali**: nessuna durabilità né supervisione remota (`APPLIED_VOLATILE`, nessun journal, nessuna replica).
14. Le prove di qualità (test, proptest, fuzz a tempo, benchmark su una VM) non sono una **prova formale**: riducono il rischio, non lo escludono.

---

## VS-003

Brief: `MoscaSCADA_VS003_Agent_Brief.md`. Base: VS-002 finale `d9502c1` (P1/P2, CI `stable` + `fuzz-smoke` verdi), poi `main` `9d97882` (merge di #1 e #2, stesso albero). Branch dedicato, PR verso `main` senza merge. Decisione: ADR 0010 §11; misure: `docs/benchmarks/vs003.md`.

### Gate §0

| Voce | Stato | Evidenza |
|---|---|---|
| PR #2, SHA finale, commit correttivi | ✅ | `add3461`, `95840c5`, `d9502c1`; CI verde su push e PR; poi merge in `main` `9d97882` (merge commit, nessuna riscrittura) |
| P1 / P2 chiusi e testati | ✅ | test VS-002 `adversarial_many_small_observations…`, `takeover_of_a_backpressured_non_reading_link…` verdi anche sotto `source_fair` (`SCADA_TEST_INGRESS`) |
| Baseline riproducibile | ✅ | binario `d9502c1` misurato nella stessa campagna (`vs002_mt2`), nessun numero di build precedenti usato come riferimento |
| Lavoro prima del gate | ✅ | solo preparazione (ADR, modello, harness, profilo su `fd5e992` dichiarato non baseline) finché il gate non è stato verde |

### Vincoli §2

| Vincolo | Stato | Evidenza |
|---|---|---|
| `Kernel::apply` puro, un solo writer, stesso modello | ✅ | core non modificato; `check.sh` audit di purezza; unico chiamante `sequencer::apply` |
| `operation_sequence` = ordine di commit; ordine/ACK per sorgente | ✅ | `vs003.rs::every_policy_keeps_per_source_order_and_replays_to_the_live_digest` (`ct`/`mt2` × 3 politiche) |
| Nessuna coda illimitata, nessun drop/coalescenza di osservazioni | ✅ | lane limitate (`scada-sched`), semafori, bound `u128`; nessun percorso di scarto |
| Wire `scada.driver.v1` invariato, nessuna dipendenza esterna nuova, nessun `unsafe` | ✅ | nessuna modifica a `proto/`; `scada-sched` senza dipendenze; audit `unsafe` |
| Profili Micro/Server, worker non derivati dalle CPU | ✅ | profili invariati (FIFO), chiavi VS-003 commentate; test `shipped_profiles_fit_their_budget_with_the_vs003_ingress_policies` |
| Default FIFO | ✅ | `IngressPolicy::Fifo` di default; test |

### Strategia §3–§4

| Voce | Stato | Evidenza |
|---|---|---|
| A FIFO / B quote / C lane RR / D DRR confrontate | ✅ / ✅ / ✅ / modello | campagna c1–c3 (532 run); D solo nel modello (beneficio −1/2 %, ADR 0010 §3.1) |
| ADR con stati, ammissione, barriera, takeover, control plane, shutdown, cancel-safety | ✅ | ADR 0010 §4 |
| Modello puro con oracle: boundedness, fair progress, no perdita/doppio, wakeup | ✅ | `crates/scada-sched/tests/model.rs` (proptest + test di mutazione), `tests/sim.rs` |
| Soluzione minima, decisione misurata | ✅ | FIFO default; C opt-in sperimentale; B rimossa (`b21dc31`) |

### Test §7

| # | Stato | Evidenza |
|---|---|---|
| 1 equivalenza core / replay | ✅ | replay dello stream catturato = digest live sotto ogni politica; digest live di run diverse **non** confrontati |
| 2 ordine per sorgente | ✅ | come sopra, con dedup/gap/rifiuti |
| 3 no perdita/duplicato | ✅ | sequenze uniche, contatori, `drain` del modello |
| 4 fair progress in turni | ✅ | `fair_policy_bounds_the_rare_source_wait_in_turns` (36 ms ≈ 18 turni contro 174 ms ≈ 87 turni, sequencer a 2 ms/richiesta, `ct` e `mt2`) |
| 5 wakeup race | ✅ | modello con finestra di race esplicita + mutazione; runtime: `Condvar` sotto lo stesso lock, semafori Tokio |
| 6 cancellazione / permit leak | ✅ | `cancelled_and_superseded_waiters_leak_no_permit` |
| 7 takeover e barriera | ✅ | `takeover_with_a_full_lane_under_fair_policy`, `disconnect_is_never_applied_before_the_same_links_batches` (stream catturato), suite VS-002 sotto `source_fair` |
| 8 congestione / payload avversario | ✅ | suite VS-002 (P1) sotto `source_fair:1` e `:4` |
| 9 introspezione | 🟡 | lane di controllo propria; interferenza non misurata separatamente |
| 10 shutdown | ✅ | `shutdown_with_sessions_waiting_for_permits` |
| 11 risorse / `u128` | ✅ | `budget::tests::ingress_policy_is_in_the_bound` (2000 sorgenti × 1024), `invalid_ingress_settings_are_refused` |
| 12 fuzz/proptest | ✅ / 🟡 | proptest del modello; fuzz VS-002 invariati (percorsi di decode non modificati), nessun target fuzz nuovo |

### Limiti e rischi residui (VS-003)

1. Target del brief **non raggiunto** su Server; il beneficio misurato esiste solo con perdita di throughput (ADR 0010 §11).
2. Misure su una VM con client co-residenti: il tail è dominato da scheduling I/O e contesa CPU, non dall'ordine del sequencer; risultato da ripetere su hardware dedicato e ARM64 (**NOT TESTED ON HARDWARE**).
3. `source_fair` costa −10…−39 % di throughput su `multi_thread` (contesa del mutex, ipotesi non profilata) e +10 % di p99 in underload `mt2`.
4. Soak ≥ 60 min, dhat e `perf` non eseguiti per `source_fair`.
5. Garanzie in **turni di servizio**, non in tempo: nessuna garanzia hard real-time.
6. `source_fair` è un esperimento opt-in **non qualificato per produzione**: condizioni di promozione e criterio di rimozione in ADR 0010 §11.6–§11.7; latenza della rara da giudicare su `rare_sched_p99`/`rare_late` (`vs003.md` §4).

## VS-004

Brief: `MoscaSCADA_VS004_Agent_Brief.md`. Base: `main` `1071547` (merge della PR #3 dopo CI `stable` + `fuzz-smoke` verdi sull'HEAD `32f3858`). Branch dedicato, PR verso `main` senza merge. Decisioni: ADR 0011; misure: `docs/benchmarks/vs004.md`. Percorsi: `rt:sup` = test unitari con tempo controllato in `crates/scada-runtime/src/supervision.rs`, `sim/` = `apps/scada-sim-driver/tests/`.

### Gate §0

| Voce | Stato | Evidenza |
|---|---|---|
| CI dell'HEAD corrente di #3 verde, nessun blocker | ✅ | `32f3858`: `stable` e `fuzz-smoke` verdi; review senza blocker aperti |
| Merge di #3 e baseline | ✅ | merge commit `1071547` in `main`; branch VS-004 creato da lì; binari `1071547` usati come baseline A/B e per la compatibilità |
| FIFO default, `source_fair` sperimentale, profili invariati | ✅ | nessuna modifica all'ingresso salvo la corsia di controllo per il `Tick`; profili: solo commenti |
| Nessun merge autonomo di VS-004 | ✅ | PR aperta per review |

### Invarianti §2

| # | Stato | Evidenza |
|---|---|---|
| 1 `apply` puro, single writer, nessun clock | ✅ | audit di purezza `check.sh`; tempo solo in `supervision.rs` (runtime) |
| 2 tempo solo come operazioni sequenziate; replay | ✅ | `S`/`R`/`X` in cattura; `rt/vs004.rs::capture_replay_reproduces_supervised_state`; `core/supervision.rs::prop_supervision_invariants` (replay); fuzz `supervision_transitions` |
| 3 memoria limitata, budget Micro/Server | ✅ | `core:kernel::supervised_apply_never_grows_internal_storage`; `rt:sup::sample_ring_is_bounded`; `rt/config_file.rs::shipped_profiles_fit_their_budget_with_vs004_supervision`; flood: `rt/vs004.rs::heartbeat_flood_is_bounded_and_does_not_starve` |
| 4 `operation_sequence` solo su `Applied` | ✅ | proptest e fuzz: no-op ⇒ snapshot, digest e op seq invariati |
| 5 default-off = VS-003 | ✅ | `rt/vs004.rs::default_off_is_the_vs003_contract`; `core/supervision.rs::unsupervised_config_keeps_v1_and_refuses_supervision_operations`; golden digest v1 invariato; A/B `vs004.md` |
| 6 `ct`, `mt2`, `source_fair`; takeover/reconnect | ✅ | `rt/vs004.rs` sotto tutte le matrici di `check.sh`; suite VS-001..003 ripetuta con supervisione attiva (`SCADA_TEST_SUPERVISION`) |

### Configurazione §3 e protocollo §4

| Voce | Stato | Evidenza |
|---|---|---|
| Chiavi per sorgente, `watchdog_poll_ms`, validazione `>= 3×poll`, overflow, incoerenze | ✅ | `rt/config_file.rs::invalid_supervision_settings_are_refused` |
| Liveness sola / freschezza sola / entrambe / nessuna | ✅ | `rt/vs004.rs` (`supervised(Some, None)` ecc.), `core/supervision.rs`, fuzz (4 sorgenti con le 4 combinazioni) |
| Heartbeat additivo, minor 1.1, `buf breaking` | ✅ | `proto/`, `scripts/check-proto.sh` verde |
| Nessun heartbeat verso kernel 1.0; driver legacy invariato | ✅ | `sim/negotiation.rs` (4 test, Hello legacy byte-identico); binari reali `1071547` ↔ VS-004: `results/vs004/compat-vs003-binaries.log` |
| Nessun ACK per heartbeat; invalidi fatali; flood coalescente | ✅ | `rt/vs004.rs::invalid_heartbeat_closes_the_link`, `heartbeat_flood_is_bounded_and_does_not_starve`; decoder: fuzz `decode_equivalence` (heartbeat incluso) |

### Semantica §5

| Voce | Stato | Evidenza |
|---|---|---|
| Silent con socket aperto, nessun disconnect fittizio | ✅ | `rt/vs004.rs::silent_open_socket_is_silent_not_disconnected`; `core/supervision.rs::silence_degrades_values_and_keeps_the_link` |
| Heartbeat ripristina comunicazione, mai `Good` | ✅ | `rt/vs004.rs::heartbeat_after_silence_restores_communication_only`, `heartbeats_keep_the_source_alive_but_never_refresh_data`; `core/supervision.rs::liveness_restoration_never_makes_a_tag_good` |
| Nessuna revisione doppia su timeout ripetuto | ✅ | `Ignored(AlreadySilent)`; proptest |
| Freschezza per tag; `Delta` parziale; `Unknown` resta `Unknown` | ✅ | `rt/vs004.rs::partial_deltas_refresh_only_their_tags`; `core/supervision.rs::freshness_is_per_tag`; `rt:sup::freshness_per_tag_with_partial_deltas` |
| Identica con nuova sequenza rinnova; `Duplicate` no; scadenza resa obsoleta ignorata | ✅ | `rt/vs004.rs::identical_values_refresh_duplicates_do_not`; `core/supervision.rs::identical_observation_renews_the_marker_and_defeats_an_older_token`; `rt:sup::renewal_just_before_the_deadline_wins` |
| Precedenza `SourceUnavailable > SourceSilent > StaleData` | ✅ | `core/supervision.rs::precedence_and_link_generations` |
| Guardie anti-stale (generazione + marker), atomicità overflow | ✅ | idem; `core:kernel::supervision_revision_overflow_is_atomic`; `supervision_kind_and_token_are_validated` |
| Takeover: timer/heartbeat vecchi innocui | ✅ | `rt/vs004.rs::takeover_old_link_cannot_keep_or_degrade_the_new_one`; `rt:sup::takeover_isolates_the_old_link` |
| Un solo watchdog, tick non affamabile | ✅ | flag controllato dopo ogni richiesta; `heartbeat_flood_is_bounded_and_does_not_starve`; jitter misurato (`vs004.md`) |
| Digest versionato; marker nel digest | ✅ | v1 invariato senza supervisione, v2 con supervisione (`core/supervision.rs::prop_digest_v2_tracks_the_snapshot`) |

### Diagnostica §6

| Voce | Stato | Evidenza |
|---|---|---|
| Liveness per fonte, ragioni distinte, contatori bounded | ✅ | `introspect.proto` additivo; contatori usati da `rt/vs004.rs` e dal soak |
| `--check-config` con sorgenti, soglie, bound | ✅ | riga `supervision:`; `READY ... supervision=off|N` |
| Indicatori logici vs metriche operative | ✅ | ADR 0011 D8/D9 |

### Test §7

| # | Stato | Evidenza |
|---|---|---|
| 1 default-off = VS-003 | ✅ | vedi §2.5 |
| 2–5 | ✅ | vedi §5 |
| 6 confini `T−ε`/`T`/`T+ε`, tick in ritardo, raffiche, saturazione, overflow | ✅ / 🟡 | `rt:sup::liveness_boundaries_are_never_early`, `late_ticks_and_evidence_stamping`, `heartbeats_keep_alive_and_restore_without_good`; overflow revisione/token nel core; sequencer saturo solo E2E (flood) e campagna, non con tempo controllato; esaurimento di `operation_sequence` = percorso VS-002 invariato |
| 7 reconnect stessa/nuova epoca, takeover | ✅ | `rt/vs004.rs::takeover_…`, `rt:sup::disconnection_stops_supervision`, fuzz `supervision_transitions` (connect con epoche 1 e 2, generazioni vecchie/future), soak (reconnect con stessa epoca) |
| 8 capture/replay | ✅ | `capture_replay_reproduces_supervised_state` (con righe `S`/`R`/`X` presenti) |
| 9 legacy ↔ nuovo | ✅ | `sim/negotiation.rs`, binari reali |
| 10 E2E `ct`/`mt2`/`source_fair` | ✅ | `check.sh` |
| 11 fuzz + input ostile | ✅ | `supervision_transitions` (nuovo), `decode_equivalence`, `frame_decode`, `batch_validation`: 60 s ciascuno, nessun crash (`results/vs004/fuzz-*.log`) |
| 12 memoria / budget | ✅ | `budget.rs` (`supervision=`), profili con e senza chiavi VS-004 |

### Limiti e rischi residui (VS-004)

1. Nessuna garanzia hard real-time: rilevamento in `[T, T + poll + ritardo del tick]`; ritardo misurato solo su VM x86_64 (`vs004.md`).
2. Freschezza = tempo dall'ultima osservazione accettata dal kernel, non età fisica della misura.
3. ARM64 / Raspberry Pi: **NOT TESTED ON HARDWARE** (solo `cargo check`).
4. Benchmark su VM con client co-residenti; ripetere su hardware dedicato prima di qualunque qualifica.
5. Supervisione **non qualificata per produzione** e non attivata nei profili; il soak (31 min, `vs004.md` §4) copre una sola configurazione (`ct`, 4 sorgenti).
6. Dopo un silenzio i tag non osservati restano `Uncertain(SourceSilent)` fino alla loro prossima osservazione (atteso; un driver dovrebbe inviare un `FullState` dopo un'interruzione).
7. Review PR #4: P1 (scansione O(tag) per tick su impianto stale) corretto con la lista di freschezza ordinata per marker, misurato fino a 1M tag (`vs004.md` §9); P2 (`liveness_recoveries` senza i recuperi per batch) corretto con test.

## VS-005

Brief: `MoscaSCADA_VS005_Alarm_Engine_Agent_Brief_v2.md`. Base: `main` `b762242` (merge commit della PR #4, HEAD `f80d900` con `stable` e `fuzz-smoke` verdi, thread P1/P2 risolti). Branch `claude/project-thread-6p8945`, PR verso `main` **senza merge** (review indipendente, G9). Decisioni: ADR 0012; misure: `docs/benchmarks/vs005.md`; catalogo: `docs/alarm-catalog.md`. Percorsi: `core/alarms.rs`, `core:kernel` (test unitari del kernel), `core:alarm` (test unitari di `alarm.rs`), `rt/vs005.rs`, `rt/config_file.rs`.

### Gate §0 / G1

| Voce | Stato | Evidenza |
|---|---|---|
| PR #4: HEAD, CI, mergeability, nessun blocker | ✅ | HEAD `f80d900`, `stable` + `fuzz-smoke` verdi sullo SHA, mergeable, thread risolti |
| Merge di #4 (merge commit) e base registrata | ✅ | `b762242`; binario `b762242` usato come baseline A/B (`results/vs005/c1-ab.txt`) |
| Nessun merge autonomo di VS-005 | ✅ | PR aperta per review |

### Invarianti §2

| # | Stato | Evidenza |
|---|---|---|
| I1 purezza, ordine totale (ACK incluso) | ✅ | audit di purezza `check.sh`; ACK/watermark = `Operation` catturate (`A`/`W`); `core/alarms.rs::t11_replay_reproduces_events_and_digest`; `rt/vs005.rs::episode_and_ack_over_ipc_and_replay`; fuzz `alarm_transitions` (replay) |
| I2 atomicità integrale | ✅ | `core/alarms.rs::t07_*` (20 000 eventi in una disconnessione, tutto o niente), `t12_model_atomicity_and_replay` (proptest), `core:kernel::event_sequence_exhaustion_is_atomic`; fuzz: snapshot/digest/outbox identici dopo ogni non-`Applied` |
| I3 nessuna perdita silenziosa | ✅ | `t08_ring_wraps_physically_never_logically_and_never_overwrites_undurable_events`; fuzz «ogni evento sopra il watermark leggibile e identico»; `rt/vs005.rs::full_outbox_refuses_whole_batch_then_recovers`, `disconnection_is_deferred_until_slots_are_released` |
| I4 bounded, zero allocazioni | ✅ | `core:kernel::alarm_apply_never_grows_internal_storage`; counting allocator: 0,000 allocazioni/op in ogni caso di `alarms-core` (`results/vs005/alarms-core.*`) |
| I5 qualità ortogonale | ✅ | `t04_quality_is_orthogonal_and_never_returns_to_normal`, `t04_bad_quality_arms_only_after_first_good`; fuzz; `rt/vs005.rs::supervision_silence_drives_bad_quality_but_never_auto_normal` |
| I6 ACK sicuro | ✅ | `t05_episode_automaton_tokens_and_stale_acks`; `rt/vs005.rs::episode_and_ack_over_ipc_and_replay` (actor = uid del peer, ACK senza instance id rifiutato), `control_plane_security`, `restart_is_a_new_instance` |
| I7 default-off | ✅ | `t01_no_alarms_keeps_v1_v2_encodings` (golden v1/v2 invariati); `rt/vs005.rs::default_off_has_no_alarm_surface`; A/B `vs005.md` §2 |
| I8 durabilità solo da watermark | ✅ | watermark sequenziato e monotono (`t08`, fuzz); sink simulato solo con flag di test (`Durability = SIMULATED_TEST_ONLY`); nessun testo dichiara durabilità |

### Architettura §3, modello §4–§6

| Voce | Stato | Evidenza |
|---|---|---|
| `AlarmEngine` puro nello stesso `apply`, nessun watcher | ✅ | `scada-core::alarm`; ADR 0012 D4 |
| CSR tag → regole, regole ordinate per id, valutazione incrementale | ✅ | `AlarmTable::build`; `core:alarm::listed_index_next_matches_a_scan`; `alarms-core` A (costo con 0/100/1k/100k regole) |
| Indice `active || unacked` senza scansione | ✅ | bitset a due livelli; `t08_snapshot_pages_list_active_or_unacked_in_id_order` |
| Preflight senza allocazioni, commit infallibile | ✅ | ADR 0012 D4; I2/I4 |
| Refresh identico `Good` senza eventi | ✅ | stage solo su cambio di (valore, qualità); `t05`/proptest |
| Regole bool/high/low/bad_quality, isteresi, `i64::MIN/MAX` | ✅ | `t03_high_low_hysteresis_and_boundaries`; `core:alarm::occurrence_counter_never_wraps` |
| Fingerprint versionato, indipendente dall'ordine TOML | ✅ | `t02_fingerprint_is_order_independent_and_semantic` |
| Config fail-fast (id duplicati/zero, tipi, soglie, tag inesistente, outbox < fan-out, limiti) | ✅ | `t02_config_validation_fails_fast`; `rt/config_file.rs::invalid_alarm_sections_are_refused`; `rt/vs005.rs::check_config_reports_alarm_capacity` |
| Automa episodi, retrigger, token stantio, ACK ripetuto | ✅ | `t05`; `rt/vs005.rs::episode_and_ack_over_ipc_and_replay` |
| Eventi ordinati per `alarm_id`, contigui, `ordinal_in_operation` | ✅ | `t06_events_of_one_operation_are_ordered_by_alarm_id_whatever_the_input_order`; fuzz |
| Slot `N` riusato solo se watermark ≥ `N`; `OutboxCapacityExceeded{required,free}` prima di mutare | ✅ | `t07`/`t08`; `alarms-core` D (C e C+1) |
| Timer VS-004 senza client: ritentati, mai inventati | ✅ | sequencer: scadenza rifiutata ⇒ nessuna operazione, ritentata al tick; `rt/vs005.rs::supervision_…` |
| Saturazione segnalata fuori dal ring | ✅ | `CapacityStatus`, `AlarmSummary`, stderr; `full_outbox_refuses_whole_batch_then_recovers` (CRITICAL) |

### Catalogo §7, control plane §8, digest §9

| Voce | Stato | Evidenza |
|---|---|---|
| Schema `AlarmCatalogEntry` + `AlarmGraphicRef` + esempio validato | ✅ | `docs/catalog/*.json`, `scripts/check_alarm_catalog.py` in `check.sh` (anche caso negativo) |
| Socket separato, uid allowlist, limiti, malformati, actor non falsificabile | ✅ | `rt/vs005.rs::control_plane_security` (8 casi), `episode_and_ack_over_ipc_and_replay` |
| Snapshot paginato coerente (`SNAPSHOT_CHANGED`), eventi con cursori/gap/`cursor_ahead` | ✅ | `control_plane_security` (pagina 1, cambio di stato), `full_outbox_refuses_whole_batch_then_recovers` (gap) |
| `GetCapacityStatus` con durabilità reale e memoria | ✅ | `alarms.proto` `CapacityStatus`; test E2E |
| Default-off v1/v2 byte-identici; v3 con allarmi | ✅ | `t01`; `rt/vs005.rs` (digest v3) |
| Cattura `A`/`W`, replay identico | ✅ | `rt/capture.rs` round-trip; `episode_and_ack_over_ipc_and_replay`; suite `SCADA_TEST_ALARMS=1` (replay vs002/vs003 con allarmi) |

### Memoria §10, benchmark §11, test §12

| Voce | Stato | Evidenza |
|---|---|---|
| Bound da `size_of` e capacità reali, `--check-config`, fail-fast | ✅ | `budget.rs` (`alarms`, `alarm_core`); `rt/config_file.rs::shipped_alarm_example_is_valid` |
| Profili Micro (4 GiB fisici) / Server | 🟡 | `config/profiles/micro-alarms.toml` (budget 64 MiB, outbox 524 288 slot) verificato da `rt/config_file.rs::micro_alarm_profile_fits_its_budget_and_the_2mib_budget_refuses_it`; esempi di dimensionamento in `vs005.md` §7; nessuna misura su hardware Micro reale |
| Capacity tool A/B/C, formule, test | ✅ | `scripts/alarm_capacity.py`, `scripts/test_alarm_capacity.py` (8 test, T15) |
| A/B VS-004 vs VS-005 disabilitato | ✅ nessuna regressione ripetibile di throughput (25/25 celle), RSS +2,5…+3,7 %; allarmi on mt2: versione iniziale −16…−26 %, finale dopo la review −1,2 % / −0,2 % n.s. (`vs005.md` §2.2–§2.3) | `results/vs005/c1-ab.txt`, `c3-opt-ab.txt`, `c4-opt-confirm.txt`, tabelle `c1-*.md`, `c3-opt-*.md`, `c4-opt-*.md` |
| Enabled: no trip, storm, fan-out, pressione, C/C+1, preflight, zero alloc, cold start | ✅ | `vs005.md` §3, `results/vs005/alarms-core.*` |
| Storm 60 s misurato PASS/FAIL + sink fermo | ✅ (`vs005.md` §4–§5) | `results/vs005/storm.txt` |
| Soak ≥ 30 min | ✅ (`vs005.md` §6) | `results/vs005/soak.txt` |
| T01–T15 | ✅ | sopra; T13 (≥ 100 000 regole: `alarms-core` A/F fino a 1 000 000 regole); T14 `restart_is_a_new_instance` |
| CI: allarmi on/off, supervisione on/off, FIFO, `source_fair`, ct/mt2, fuzz smoke | ✅ | `scripts/check.sh`, `.github/workflows/ci.yml` (`alarm_transitions` 60 s) |
| ARM64 | 🟡 | solo `cargo check` (**NOT TESTED ON HARDWARE**) |

### Limiti e rischi residui (VS-005)

1. **Nessuna durabilità** e **NON PRODUCTION-READY FOR LONG-RUN ALARM RETENTION**: senza journal (VS-006) gli slot non si liberano; dopo `C` eventi ogni nuova transizione d'allarme è rifiutata fino al riavvio.
2. Input rifiutati per capacità possono andare persi **fuori dal kernel** se il driver non li conserva (contratto §9.4).
3. Disconnessione differita: il link resta `Connected` nel kernel finché non si liberano slot (dichiarato da `pending_link_transitions`).
4. Regole a tag singolo; regole multi-tag (VS-007) richiederanno deduplicazione per regola nel preflight.
5. Benchmark su VM x86_64 con client co-residenti; ARM64 NOT TESTED ON HARDWARE.
6. `actor_uid` è l'identità di un processo, non di una persona.

## VS-006

Brief: `MoscaSCADA_VS006_Durable_Journal_Recovery_Time_Authority_Agent_Brief.md`. Base: `main` `9f335df` (merge commit della PR #5; HEAD della PR `a5ec3ac` con `stable` e `fuzz-smoke` verdi sullo SHA). Branch `claude/project-thread-uymxzt`, PR verso `main` **senza merge** (review tecnica indipendente). Decisioni: ADR 0013, ADR 0014 (scritti prima delle modifiche strutturali, commit `d396bd9`); misure: `docs/benchmarks/vs006.md` e `docs/benchmarks/results/vs006/`; esercizio: `docs/operations/vs006-playbook.md`. Percorsi: `j/` = `crates/scada-journal/tests/journal.rs`, `j:codec` / `j:reconcile` = test unitari di `scada-journal`, `core/recovery.rs`, `rt/vs006.rs`, `rt:persist` / `rt:time` = test unitari di `persist.rs` / `time_authority.rs`.

Legenda della matrice T01–T30: **PASS** = eseguito e superato in questo repository; **PARTIAL** = eseguito, ma prova solo una parte della proprietà (motivo indicato); **SKIPPED** = non eseguibile qui (motivo indicato). Nessuna riga è FAIL alla consegna.

### Gate §0

| Voce | Stato | Evidenza |
|---|---|---|
| PR #5: HEAD, CI, review | ✅ | HEAD `a5ec3ac`; check run `stable` e `fuzz-smoke` `success`; nessun blocker aperto |
| Merge di #5 e base registrata | ✅ | merge commit `9f335df`; binario `9f335df` usato come baseline A/B (`results/vs006/ab.txt`, variante `A`) |
| ADR 0013/0014 prima del codice strutturale | ✅ | `d396bd9` precede `2c6bc35` (primo commit di codice) |
| Nessun merge autonomo di VS-006 | ✅ | PR aperta per review |

### Invarianti J1–J12

| # | Stato | Evidenza |
|---|---|---|
| J1 autorità del commit | ✅ | `rt/vs006.rs::durable_receipts_follow_the_commit`, `slow_disk_delays_durable_receipts`, `crash_at_every_fault_point_recovers_a_coherent_prefix` (nessuna receipt durevole persa in 24 crash), `kill9_during_a_storm_recovers_coherently_every_time`; `normal_durability_is_benchmark_only` (NORMAL non emette mai receipt durevoli) |
| J2 totalità causale | ✅ | una transazione per gruppo di operazioni intere (writer); `j/recovery_equals_live_kernel`, `missing_operation_or_event_is_fail_closed`; T05 |
| J3 ring protetto | ✅ | watermark solo dal writer dopo `COMMIT`; `rt/vs006.rs::journal_releases_the_outbox_ring`, `disk_full_refuses_and_publishes_nothing`; sink simulato rifiutato con `[persistence]` |
| J4 ammissione prima della mutazione | ✅ | `rt:persist::admission_at_the_exact_queue_edge`, `disk_floor_and_fault_refuse_before_apply`; `rt/vs006.rs::stalled_disk_backpressure_then_recovery`, `disk_full_refuses_and_publishes_nothing`, `eio_is_a_fault_until_restart` |
| J5 recovery ≠ verifica | ✅ | `core/recovery.rs::fence_semantics_no_false_events`, `recovered_unverified_has_top_precedence`; `rt/vs006.rs::restart_recovers_unverified_then_reconciles`, `partial_reconnect_verifies_only_that_source` |
| J6 nessun falso evento | ✅ | `fence_semantics_no_false_events`; `rt/vs006.rs::replay_emits_nothing`; rientri marcati `reconciliation` (`restart_recovers_unverified_then_reconciles`) |
| J7 tempo non causale | ✅ | `rt/vs006.rs::clock_steps_are_diagnosed_not_causal`, `cross_boot_regression_is_detected`, `time_zone_does_not_change_stored_times`; `rt:time::clock_steps_are_flagged_and_logical_is_monotonic` |
| J8 buco storico esplicito | ✅ | `rt/vs006.rs::continuity_gap_and_new_epoch`; `j:reconcile::legacy_rules`, `buffered_complete_only_with_proof`, `buffered_without_baseline_is_unknown_and_overflow_is_gap` |
| J9 recupero bounded | ✅ | `j/recovery_equals_live_kernel` (replay < 60 op con checkpoint), `historian_pages_beyond_the_ring`; recovery dopo milioni di operazioni misurato (`vs006.md`) |
| J10 compatibilità / default-off | ✅ | `rt/vs006.rs::default_off_is_the_vs005_contract`, `driver_versions_against_a_journaling_kernel`; suite VS-001…VS-005 invariate; A/B `B` vs `A` (`vs006.md` §2) |
| J11 operatori e identità | ✅ | `rt/vs006.rs::durable_operator_ack_is_idempotent_across_restart`, `require_durable_without_journal_is_refused`; `actor_uid` resta uid di processo |
| J12 nessun falso garante end-to-end | ✅ | nessuna continuità `Complete` per driver legacy (`legacy_rules`); `SourceEventEnvelope` solo schema (ingest nativo differito, dichiarato) |

### Matrice T01–T30

| ID | Esito | Evidenza / motivo |
|---|---|---|
| T01 PR #5, base, default-off | PASS | gate §0; `default_off_is_the_vs005_contract`; golden digest VS-004/005 (suite `vs004.rs`, `vs005.rs` invariate); A/B `B` vs `A` (`vs006.md` §2) |
| T02 commit 1 op / 0 eventi | PASS | `j/recovery_equals_live_kernel`, `genesis_replay_and_resume`; `rt/vs006.rs::restart_recovers_unverified_then_reconciles` (FullState/delta restaurati dopo crash); bench journal E=0 |
| T03 fan-out 20 000 | PASS | bench journal `F20000_disk_full` (`results/vs006/journal.txt`): 20 000 eventi per operazione in una transazione, recovery con digest uguale al live; atomicità core VS-005 `t07` |
| T04 ACK + ReturnedUnacked dopo replay | PASS | `core/recovery.rs::checkpoint_restore_is_transparent` (proptest 256 casi), `rt/vs006.rs::durable_operator_ack_is_idempotent_across_restart`, `restart_recovers_unverified_then_reconciles` |
| T05 kill -9 prima/durante/dopo COMMIT | PASS | `crash_at_every_fault_point_recovers_a_coherent_prefix`: 8 punti (`before_apply`, `after_apply_before_enqueue`, `after_enqueue`, `before_transaction`, `during_transaction`, `after_commit_before_watermark`, `during_checkpoint`, `after_durable_ack_before_socket_write`) × 3 posizioni; `kill9_during_a_storm_recovers_coherently_every_time` (5 cicli) |
| T06 power-cut simulato / fault injection | PARTIAL | `torn_wal_tail_gives_a_prefix_or_a_refusal` (WAL troncato/corrotto dopo kill -9 ⇒ prefisso o rifiuto), `j/corruption_is_fail_closed`. **Simulazione**: nessun taglio di corrente reale; la durabilità reale dipende dal rispetto del flush da parte di disco/hypervisor |
| T07 writer lento/fermo, ENOSPC, EIO | PASS | `slow_disk_delays_durable_receipts`, `stalled_disk_backpressure_then_recovery`, `disk_full_refuses_and_publishes_nothing`, `eio_is_a_fault_until_restart`; `j/sqlite_full_publishes_nothing_and_recovers`, `eio_is_a_fault`; storm 60 s lenta/ferma (`results/vs006/storm.txt`) |
| T08 operazione al bordo di ring/coda | PASS | coda journal: `rt:persist::admission_at_the_exact_queue_edge` (ops e byte esatti, rifiuto intero senza prenotazione); ring: VS-005 `t07`/`alarms-core` D (C e C+1) |
| T09 replay oltre C eventi, checkpoint | PASS | `j/recovery_equals_live_kernel` (ring 8, centinaia di eventi); bench `journal-recovery` con milioni di eventi e ring 65 536 (`digest_equal=true`) |
| T10 checkpoint interrotto/corrotto/incompatibile | PASS | `j/corruption_is_fail_closed` (fallback al precedente, poi rifiuto), `incompatible_store_is_refused`, `replay_payload_compaction_is_explicit_and_fail_closed`; fault `during_checkpoint` (T05); `configuration_change_needs_an_explicit_new_timeline` |
| T11 restart con Good e ActiveUnacked/ActiveAcked | PASS | `fence_semantics_no_false_events`; `restart_recovers_unverified_then_reconciles` |
| T12 sorgente 1 rientra, 2 offline | PASS | `partial_reconnect_verifies_only_that_source` |
| T13 FullState senza buffer storico | PASS | `continuity_gap_and_new_epoch` (Unknown/Gap persistiti, mai Complete); `legacy_rules` |
| T14 buffered replay con progressivi e duplicati | PARTIAL | `j:reconcile::buffered_complete_only_with_proof`, `buffered_without_baseline_is_unknown_and_overflow_is_gap` (tracker di componente). Ingest wire di eventi nativi **differito** (ADR 0013 D10): non c'è ancora un driver che invii uno stream bufferizzato |
| T15 clock −1 s, −1 h, +24 h sotto carico | PASS | `clock_steps_are_diagnosed_not_causal` (orologio iniettato: **NOT TESTED WITH A REAL OS CLOCK STEP**); `rt:time::clock_steps_are_flagged_and_logical_is_monotonic`, `idle_is_not_a_jump` |
| T16 DST e UTC a offset | PASS | `time_zone_does_not_change_stored_times` (TZ diversi ⇒ stessi tempi raw e digest); DST è solo visualizzazione (ADR 0014 §4) |
| T17 RTC arretrato al reboot, monotonic reset | PASS | `cross_boot_regression_is_detected`, `rt:time::cross_boot_regression` (logical non regredisce, `boot` cambia, nessuna sottrazione fra boot) |
| T18 timestamp di sorgente fuori ordine/tardivi/assenti | PASS | `source_timestamps_are_kept_unchanged` (ordine per sequenza, originali invariati, `null` = assente, duplicato non sostituisce); `INVALID_TIMESTAMP` |
| T19 skew NTP e watchdog | PASS | `watchdog_is_independent_of_the_wall_clock` (orologio iniettato) |
| T20 driver 1.0/1.1/1.2 | PASS | `driver_versions_against_a_journaling_kernel`; `apps/scada-sim-driver/tests/negotiation.rs` |
| T21 ACK durevole perso dopo commit, retry stesso id | PASS | `durable_operator_ack_is_idempotent_across_restart` (stesso esito ed `event_seq`, un solo evento storico) |
| T22 historian 10 M+ eventi, ring 64k | PASS | bench `rows10M_*` (`results/vs006/journal.txt`): pagine keyset bounded, query per allarme con indice; `historian_pages_beyond_the_ring` |
| T23 disco e WAL a cap / compaction | PASS | WAL troncato oltre `wal_max_bytes`; `disk_full_refuses_and_publishes_nothing`; compattazione esplicita `replay_payload_compaction_is_explicit_and_fail_closed`. Nessuna retention automatica dello storico (dichiarato) |
| T24 soak 30–60 min e ripartenza durante storm | PASS | `kill9_during_a_storm_recovers_coherently_every_time`; soak (`results/vs006/soak.txt`, `vs006.md`) |
| T25 replay deterministico con TimeAuthority iniettato | PASS | `checkpoint_restore_is_transparent`; `replay_emits_nothing`; `time_zone_does_not_change_stored_times` (stessi digest e tempi raw dal journal) |
| T26 driver non-buffering durante crash | PASS | `continuity_gap_and_new_epoch`, `restart_recovers_unverified_then_reconciles` (Unknown osservabile, nessun Complete) |
| T27 recovery senza Logic Engine/comandi | PASS | `replay_emits_nothing` (nessun evento, nessun messaggio ai driver durante il recovery); nessun Logic Engine esiste in VS-006 |
| T28 socket, path, lock, owner, permessi | PASS | `single_writer_and_private_store`, `j/single_writer_and_permissions`; socket historian 0660 + allowlist uid |
| T29 property/fuzz | PASS | `j:codec` proptest (`op_roundtrip_and_strict`, `decoded_is_canonical`, `decoders_never_panic`), `checkpoint_restore_is_transparent`; fuzz `journal_decode` 300 s, 0 crash (`results/vs006/fuzz_journal_decode.md`), 60 s in CI |
| T30 ARM64 | PARTIAL | `cargo check --target aarch64-unknown-linux-gnu` in `check.sh`; **NOT TESTED ON HARDWARE** (nessun hardware Micro disponibile) |

### Limiti e rischi residui (VS-006)

1. **Power loss reale non provato**: kill -9 e WAL troncato simulato sì; un taglio di corrente su hardware no. Le misure sono su un disco di VM: il costo e il valore dell'fsync sono quelli dell'hypervisor.
2. **ARM64 / Micro NOT TESTED ON HARDWARE**: la capacità del journal (μ) su SD/eMMC va misurata sul campo (`scripts/alarm_capacity.py` livello A).
3. Con persistenza attiva il throughput è limitato dal journal (ogni osservazione è registrata): costo misurato in `vs006.md`; oltre la capacità i driver ricevono `JOURNAL_QUEUE_FULL` (backpressure esplicita, mai perdita silenziosa nel kernel).
4. Ingest di eventi nativi bufferizzati differito: `Complete` non è mai dichiarato per i driver attuali.
5. Nessuna retention automatica dello storico allarmi; compattazione dei payload di replay solo esplicita.
6. Prove di salto d'orologio con orologio iniettato, non con uno step reale del clock del SO.
7. `actor_uid` resta l'identità di un processo; `principal` autenticato non implementato.

