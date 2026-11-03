# moscascada — SCADA Engine, vertical slices VS-001 + VS-002 + VS-003 + VS-004 + VS-005 + VS-006

Percorso `driver esterno → IPC → validazione → sequencer → state machine deterministica → RTDB in RAM`, in Rust stable, Linux-first.

**Stato:** VS-001 e VS-002 implementate e verificate su Linux x86_64 (VM KVM, 4 vCPU). VS-002 rende multicore il bordo di ingestion (Tokio `multi_thread` opzionale, sequencer e `Kernel::apply` invariati, un solo writer), ottimizza il percorso IPC e il lookup nel core, aggiunge liveness del writer, chiusura del link superato, procedura di ripresa con fotografia fresca, bound configurativo della memoria e hardening del socket. ARM64: **NOT TESTED ON HARDWARE** (solo `cargo check`). Lo stato è solo in RAM: nessun ACK è durevole; nessuna garanzia hard real-time; nessuna ridondanza.

**VS-003** (ADR 0010, [vs003.md](docs/benchmarks/vs003.md)): politica di ingresso esplicita verso l'unico writer, `[ipc] ingress_policy = "fifo"` (default, VS-002, invariato) | `"source_fair"` (sperimentale: lane per sorgente servite round-robin), con modello puro dello scheduler (`crates/scada-sched`), test di ordine/barriera/cancellazione/fair progress sotto entrambe le politiche e campagna A/B/C misurata prima della scelta. **Decisione:** l'obiettivo del brief (p99 rara ~2× a throughput −10 %) non è raggiunto da nessuna variante sul profilo Server; FIFO resta il default; `source_fair` è un esperimento opt-in, **non qualificato per produzione** (beneficio solo nel caso Micro saturo, con costo di throughput dichiarato). Condizioni per promuoverlo e criterio di rimozione: ADR 0010 §11.

**VS-004** (ADR 0011, [vs004.md](docs/benchmarks/vs004.md)): supervisione **opzionale, disabilitata per default** delle sorgenti. Per sorgente `liveness_timeout_ms` (driver muto con socket aperto → sorgente `Silent`, tag `Uncertain(SourceSilent)`, link ancora `Connected`) e `freshness_timeout_ms` (ogni tag non osservato da `T` → `Uncertain(StaleData)`, valutato **per tag**); un solo thread watchdog (`[runtime] watchdog_poll_ms`). Le scadenze entrano nel core come **operazioni sequenziate** con guardie senza tempo (generazione del link, token in operation sequence), catturate e riprodotte senza clock; nessun clock in `Kernel::apply`. Protocollo **1.1** additivo: `Heartbeat` (nessun ACK, coalescente, mai in coda), negoziato col minor: un driver aggiornato non invia heartbeat a un kernel 1.0, un driver legacy funziona invariato. Un heartbeat ripristina la comunicazione, **mai la qualità**: solo un'osservazione applicata porta un tag a `Good`. A supervisione disattivata contratto, digest (v1) e prestazioni sono quelli di VS-003 (A/B in vs004.md). ARM64: **NOT TESTED ON HARDWARE**.

**VS-005** (ADR 0012, [vs005.md](docs/benchmarks/vs005.md)): **motore allarmi deterministico, opzionale e disabilitato per default** (sezione `[alarms]` assente ⇒ comportamento, digest e prestazioni di VS-004). Regole statiche per tag (`bool_equals`, `high`/`low` con isteresi, `bad_quality`), severità, id stabili; stato compatto per regola, episodi `Normal → ActiveUnacked → ActiveAcked/ReturnedUnacked`, retrigger, ACK sequenziato con token anti-stale e uid del peer. Valutazione **dentro `Kernel::apply`**: preflight senza allocazioni (un solo passaggio con una finestra «quiet» esatta per tag: a riposo, regole attive non costano throughput misurabile, vs005.md §2.3), poi commit unico di RTDB + allarmi + eventi; una regola di processo con input non `Good` è *Indeterminate*, mai rientrata automaticamente. **Event Outbox** limitato con ordine totale e **nessuna sovrascrittura di eventi non durevoli**: quando gli slot non bastano l'intera operazione è rifiutata (`OUTBOX_CAPACITY_EXCEEDED`, stato CRITICAL), mai applicata a metà. Socket di controllo separato `scada.alarm.v1` (snapshot paginato, lettura eventi con cursori/gap, ACK, capacità). **Nulla è durevole** in VS-005 e senza il journal (VS-006) gli slot non tornano mai liberi: **NON PRODUCTION-READY FOR LONG-RUN ALARM RETENTION**. Dimensionamento: `scripts/alarm_capacity.py`; catalogo operatore esterno: [docs/alarm-catalog.md](docs/alarm-catalog.md). ARM64: **NOT TESTED ON HARDWARE**.

**VS-006** (ADR 0013, ADR 0014, [vs006.md](docs/benchmarks/vs006.md), [playbook](docs/operations/vs006-playbook.md)): **journal durevole opzionale, disabilitato per default** (sezione `[persistence]` assente ⇒ comportamento, digest e ACK di VS-005). Con `[persistence]`: store SQLite (WAL, `synchronous=FULL`, un solo writer con lock, file `0600`), ammissione nella coda journal **prima** di `Kernel::apply` (rifiuti tipizzati `JOURNAL_QUEUE_FULL`/`DISK_FULL`/`JOURNAL_FAULT`, nulla applicato), writer dedicato con group commit, watermark dell'outbox avanzato **solo** dopo il `COMMIT` (gli slot degli allarmi tornano liberi), checkpoint completi e recovery fail-closed (checkpoint + replay verificato evento per evento). Il riavvio distingue **stato recuperato** (valori `Uncertain(RecoveredUnverified)`, sorgenti non verificate fino a un `FullState`), **stato live verificato** e **continuità degli eventi** (`Unknown`/`Gap` persistiti, mai `Complete` senza prova); nessun falso `Good`, `Returned` o ACK; i rientri osservati alla riconciliazione sono marcati. Protocollo driver **1.2** additivo: receipt `APPLIED_DURABLE` (dopo il commit) su richiesta, timestamp di sorgente opzionali conservati invariati. ACK operatore idempotenti (`action_id`) e opzionalmente durevoli. Historian allarmi (`scada.history.v1`, pagine keyset oltre il ring, proiezione attiva `as_of`) e diagnostica separata. **Time Authority**: ordine sempre per sequenza, tempo raw + chiave logica non regressiva + qualità, regressioni e salti diagnosticati, non causali. Garanzie provate con kill -9 (8 punti di guasto e kill casuali durante una storm), WAL troncato **simulato**, disco lento/fermo/pieno/EIO iniettati; **nessun test di power loss reale**, misure solo su disco di VM; ARM64 **NOT TESTED ON HARDWARE**.

Documenti:
- [Contratto semantico](docs/contract.md) · [Architettura](docs/architecture.md)
- ADR: [0001 IPC](docs/adr/0001-ipc.md) · [0002 stato del kernel e digest](docs/adr/0002-kernel-state.md) · [0003 eccezione `unsafe` nel benchmark](docs/adr/0003-bench-allocator-unsafe.md) · [0004 runtime Tokio](docs/adr/0004-tokio-runtime.md) · [0005 drain del sequencer (misurato, non adottato)](docs/adr/0005-sequencer-drain.md) · [0006 allocazioni e IPC](docs/adr/0006-allocations-ipc-path.md) · [0007 lookup/sort nel core](docs/adr/0007-core-lookup-sort.md) · [0008 liveness, takeover, ripresa](docs/adr/0008-liveness-takeover-reconnect.md) · [0009 memoria e sicurezza UDS](docs/adr/0009-memory-budget-uds-security.md) · [0010 ingresso fair (VS-003)](docs/adr/0010-fair-ingress.md) · [0011 liveness e freschezza (VS-004)](docs/adr/0011-source-liveness-and-data-freshness.md) · [0012 motore allarmi, outbox e capacità (VS-005)](docs/adr/0012-alarm-engine-event-outbox-and-capacity.md) · [0013 journal durevole e recovery (VS-006)](docs/adr/0013-durable-journal-and-recovery.md) · [0014 Time Authority e tempo di sorgente (VS-006)](docs/adr/0014-time-authority-and-source-time.md)
- Benchmark: [VS-001](docs/benchmarks/vs001.md) · [VS-002 (confronti, campagne, analisi critica)](docs/benchmarks/vs002.md) · [VS-003 (politiche di ingresso)](docs/benchmarks/vs003.md) · [VS-004 (supervisione: default-off vs attiva)](docs/benchmarks/vs004.md) · [VS-005 (allarmi: A/B, storm, saturazione, capacità)](docs/benchmarks/vs005.md) · [VS-006 (journal: A/B, durabilità, historian, recovery, capacità)](docs/benchmarks/vs006.md)
- [Playbook operativo VS-006: blackout, sorgente non sincronizzata, schema incompatibile, disco pieno, storia sconosciuta](docs/operations/vs006-playbook.md)
- [Catalogo allarmi: schema ed esempio validato](docs/alarm-catalog.md)
- [Matrice di tracciabilità dei requisiti, limiti noti e valutazione critica](docs/traceability.md)

## Struttura

```text
Cargo.toml / Cargo.lock / rust-toolchain.toml   workspace, lockfile, toolchain fissata (1.97.0)
config/demo.toml                                configurazione statica della demo (sorgente 1001, tag 101-103)
config/profiles/micro.toml | server.toml        profili VS-002: current_thread / multi_thread(2), budget di memoria esplicito
config/profiles/micro-alarms.toml               VS-005: Micro (4 GiB fisici) con allarmi, budget di processo 64 MiB
config/examples/alarms.toml                     VS-005: esempio commentato con [alarms] e regole
config/examples/persistence.toml                VS-006: esempio con [persistence] e [time] (ogni chiave col suo default)
proto/scada/driver/v1/driver.proto              protocollo kernel <-> driver (versionato)
proto/scada/alarm/v1/alarm.proto                VS-005: socket di controllo allarmi (snapshot, eventi, ACK, capacità)
proto/scada/history/v1/history.proto            VS-006: historian e diagnostica (storico, proiezione, recovery, journal, tempo)
proto/scada/introspect/v1/introspect.proto      introspezione read-only per test/benchmark
proto/baseline/scada-v1.0.binpb                 immagine Buf di riferimento per `buf breaking`
buf.yaml                                        regole buf lint (STANDARD) e breaking (FILE)
crates/scada-core/                              state machine pura + RTDB + digest (+ test unitari/property)
crates/scada-protocol/                          tipi generati + framing limitato (+ test)
crates/scada-runtime/                           binario scada-kernel: UDS, sessioni, ingresso, sequencer (+ test multiprocesso)
crates/scada-sched/                             VS-003: modello puro dello scheduler di ingresso + simulatore politiche
crates/scada-journal/                           VS-006: store SQLite, writer, recovery, historian, CLI offline scada-journal
apps/scada-sim-driver/                          processo driver simulato
apps/scada-bench/                               benchmark riproducibili
fuzz/                                           workspace cargo-fuzz separato (nightly) + seed corpus
scripts/check.sh | check-proto.sh | bench.sh    gate completo, verifica schemi, benchmark VS-001
scripts/bench_campaign.py | bench_analyze.py    campagne A/B randomizzate (VS-002) e analisi (mediana, dispersione, permutazione)
scripts/micro_ab.py | criterion_table.py        microbenchmark Criterion interlacciati
scripts/alarm_capacity.py (+ test_)             VS-005/006: capacità di outbox, coda journal e disco (misurato / ipotesi / calcolato)
scripts/bench_vs006.sh                          VS-006: campagne A/B, journal, storm
scripts/check_alarm_catalog.py                  VS-005: validazione del catalogo operatore contro la configurazione
docs/catalog/                                   VS-005: schema JSON del catalogo allarmi + esempio
deny.toml                                       politica cargo-deny (licenze, advisories, sorgenti)
```

## Prerequisiti

- Rust 1.97.0 (installato automaticamente da `rustup` tramite `rust-toolchain.toml`), target opzionale `aarch64-unknown-linux-gnu`.
- Nessun `protoc` di sistema: i `.proto` sono compilati da `protox` (Rust) nel `build.rs`.
- Per la verifica degli schemi: Buf 1.75.0 (`npm install -g @bufbuild/buf@1.75.0` oppure binario ufficiale).
- Per il fuzzing: toolchain nightly e `cargo install cargo-fuzz --version 0.13.2`.

## Build, test e verifiche

```bash
cargo build --workspace --release
cargo test --workspace            # unit, property-based, integrazione multiprocesso
./scripts/check.sh                # fmt, clippy -D warnings, test, audit unsafe/dipendenze/purezza, buf, aarch64
./scripts/check-proto.sh          # buf lint + buf breaking contro la baseline + prova che un breaking viene rilevato
```

Fuzzing (separato dai test stable):

```bash
cd fuzz
mkdir -p corpus && cp -r seeds/* corpus/
cargo +nightly fuzz run -O frame_decode     corpus/frame_decode     -- -max_total_time=300
cargo +nightly fuzz run -O batch_validation corpus/batch_validation -- -max_total_time=300
cargo +nightly fuzz run -O decode_equivalence corpus/decode_equivalence -- -max_total_time=300   # VS-002 C3 (+ heartbeat VS-004)
cargo +nightly fuzz run -O supervision_transitions corpus/supervision_transitions -- -max_total_time=300   # VS-004
cargo +nightly fuzz run -O alarm_transitions corpus/alarm_transitions -- -max_total_time=300   # VS-005
```

Test runtime con lo scheduler multi-thread (la CI esegue entrambi): `cargo build --release -p scada-sim-driver && SCADA_TEST_SCHEDULER=multi_thread:2 cargo test --release -p scada-runtime`.

Test runtime sotto una politica di ingresso VS-003 (la CI esegue `source_fair:4` e `source_fair:1`): `SCADA_TEST_INGRESS=source_fair:4 cargo test --release -p scada-runtime`. Suite runtime con la supervisione VS-004 attiva sulla sorgente 1001 (la CI la esegue con timeout larghi): `SCADA_TEST_SUPERVISION=60000:60000 cargo test --release -p scada-runtime` (`LIVENESS_MS:FRESHNESS_MS`, `0` = disattivato). Suite runtime con allarmi VS-005 su ogni tag (la CI la esegue con FIFO/`source_fair`, entrambi gli scheduler, con e senza supervisione): `SCADA_TEST_ALARMS=1 cargo test --release -p scada-runtime`. Suite runtime con il journal VS-006 attivo in ogni kernel di test (la CI la esegue da sola, con allarmi + `multi_thread(2)` + `source_fair:4`, e con allarmi + supervisione): `SCADA_TEST_PERSISTENCE=1 cargo test --release -p scada-runtime`; le suite che verificano un contratto volatile specifico (riferimento VS-001, digest v1/v2/v3, riavvio volatile) restano fissate senza persistenza. Modello dello scheduler: `cargo test -p scada-sched`; simulatore delle politiche: `cargo run --release -p scada-sched --example policy_model`.

## Esecuzione della demo

```bash
cargo build --release --workspace
./target/release/scada-kernel --config config/demo.toml --introspection-socket /tmp/scada-vs001/intro.sock &
./target/release/scada-sim-driver --socket /tmp/scada-vs001/driver.sock demo
```

Il simulatore stampa ogni risposta del kernel: `FullState`, `Delta`, duplicato, gap, batch invalido, disconnessione, reconnect con ritrasmissione dell'ultimo batch e ripresa dalla sequenza corretta. Lo stesso scenario è automatizzato in `crates/scada-runtime/tests/ipc.rs::demo_scenario_with_separate_driver_process`.

Opzioni del kernel:

```text
scada-kernel --config FILE [--socket PATH] [--introspection-socket PATH]
             [--scheduler current_thread|multi_thread] [--worker-threads N]   # override di [runtime]
             [--read-buffer-bytes N] [--ack-coalesce-max N]  # override (A/B)
             [--diag [--diag-probe-source ID]]  # istogrammi per stadio, letti via introspezione
             [--capture-inputs FILE]       # diagnostica: registra gli input sequenziati (replay nei test)
             [--throttle-sequencer-us N]   # solo test di saturazione: ritardo artificiale nel sequencer
             [--test-simulated-durable-sink]  # SOLO TEST (VS-005): watermark simulato via socket allarmi, NON durevole
             [--check-config]              # valida la configurazione, stampa il bound di memoria ed esce
```

Profili VS-002 (stesso codice, stesso protocollo):

```bash
./target/release/scada-kernel --config config/profiles/micro.toml  --check-config
./target/release/scada-kernel --config config/profiles/server.toml --check-config
# esecuzione con socket di prova (la directory deve essere privata: non /tmp)
mkdir -m 700 -p /tmp/moscascada-demo
./target/release/scada-kernel --config config/profiles/server.toml --socket /tmp/moscascada-demo/driver.sock &
./target/release/scada-sim-driver --socket /tmp/moscascada-demo/driver.sock demo
```

Il kernel termina con `SIGTERM`/`SIGINT`; lo stato volatile viene scartato.

Supervisione VS-004 (opzionale): aggiungere `[runtime] watchdog_poll_ms = 250` e, nella sorgente, `liveness_timeout_ms` / `freshness_timeout_ms` (chiavi commentate nei profili); `--check-config` stampa sorgenti sorvegliate, soglie e bound di memoria. Simulatore:

```bash
./target/release/scada-sim-driver --socket S --heartbeat-ms 1000 hold        # FullState, poi solo heartbeat
./target/release/scada-sim-driver --socket S heartbeat --count 20 --heartbeat-ms 500
./target/release/scada-sim-driver --socket S --heartbeat-ms 1000 soak --duration-ms 600000 --cycle-ms 200 --pause-ms 8000
./target/release/scada-sim-driver --socket S --legacy-hello demo            # Hello 1.0 byte-identico a VS-003
```

Allarmi VS-005 (opzionali): vedere `config/examples/alarms.toml`; `--check-config` stampa regole, worst fan-out, memoria del core e del control plane e `durability=NONE`. Client di prova del socket di controllo: `scada_sim_driver::AlarmClient` (usato da test e benchmark). Dimensionamento:

```bash
python3 scripts/alarm_capacity.py --bench-results docs/benchmarks/results/vs005/storm.txt \
    --outbox-slots 1048576 --event-rate 200 --storm-seconds 600 --reserve 4096
```

Journal VS-006 (opzionale): vedere `config/examples/persistence.toml` e il [playbook](docs/operations/vs006-playbook.md). La directory dello store deve essere privata. `--check-config` stampa la riga `persistence:` e la memoria per voce (coda journal, staging, checkpoint, sessioni historian, cache SQLite, azioni) con la percentuale di 4 GiB. Strumento offline:

```bash
scada-journal info   /var/lib/scada/journal/kernel.db     # timeline, watermark durevoli, checkpoint, stato sorgenti
scada-journal verify /var/lib/scada/journal/kernel.db     # + integrity_check completo
scada-journal history /var/lib/scada/journal/kernel.db --limit 50
scada-journal gaps   /var/lib/scada/journal/kernel.db     # intervalli Unknown/Gap persistiti
python3 scripts/alarm_capacity.py --bench-results docs/benchmarks/results/vs006/journal.txt \
    --bench-results docs/benchmarks/results/vs006/storm.txt --outbox-slots 65536 \
    --event-rate 1000 --events-per-op 10 --disk-free-mib 20000
```

## Benchmark

```bash
./scripts/bench.sh       # scrive docs/benchmarks/results/<host>-<data>.md
```

Metodologia e risultati misurati: [docs/benchmarks/vs001.md](docs/benchmarks/vs001.md), [docs/benchmarks/vs002.md](docs/benchmarks/vs002.md) (campagne riproducibili con `scripts/bench_campaign.py`; comandi esatti nel documento).

## Toolchain usata per i risultati riportati

| Strumento | Versione |
|---|---|
| rustc / cargo | 1.97.0 (2d8144b78 2026-07-07) / 1.97.0 |
| rustc nightly (solo fuzz) | cargo 1.101.0-nightly (31d3246e0 2026-10-27) |
| cargo-fuzz | 0.13.2 (VS-004/VS-005: stesso nightly `cargo 1.101.0-nightly (31d3246e0 2026-10-27)`) |
| protoc | non usato (compilatore `protox` 0.10.0, `prost-build` 0.14.4) |
| Buf | 1.75.0 |
| Benchmark | harness proprio `scada-bench` (nessun Criterion: servono percentili p99.9 esatti e conteggio allocazioni) |
| OS | Linux 6.18.44 x86_64 (VM KVM, 4 vCPU Intel Xeon @ 2.80GHz, 15 GiB) |
