# moscascada — SCADA Engine, vertical slices VS-001 + VS-002 + VS-003

Percorso `driver esterno → IPC → validazione → sequencer → state machine deterministica → RTDB in RAM`, in Rust stable, Linux-first.

**Stato:** VS-001 e VS-002 implementate e verificate su Linux x86_64 (VM KVM, 4 vCPU). VS-002 rende multicore il bordo di ingestion (Tokio `multi_thread` opzionale, sequencer e `Kernel::apply` invariati, un solo writer), ottimizza il percorso IPC e il lookup nel core, aggiunge liveness del writer, chiusura del link superato, procedura di ripresa con fotografia fresca, bound configurativo della memoria e hardening del socket. ARM64: **NOT TESTED ON HARDWARE** (solo `cargo check`). Lo stato è solo in RAM: nessun ACK è durevole; nessuna garanzia hard real-time; nessuna ridondanza.

**VS-003** (ADR 0010, [vs003.md](docs/benchmarks/vs003.md)): politica di ingresso esplicita verso l'unico writer, `[ipc] ingress_policy = "fifo"` (default, VS-002) | `"source_capped"` (quota per sorgente) | `"source_fair"` (lane per sorgente servite round-robin), con modello puro dello scheduler (`crates/scada-sched`), test di ordine/barriera/cancellazione/fair progress sotto ogni politica e campagna A/B/C misurata prima della scelta.

Documenti:
- [Contratto semantico](docs/contract.md) · [Architettura](docs/architecture.md)
- ADR: [0001 IPC](docs/adr/0001-ipc.md) · [0002 stato del kernel e digest](docs/adr/0002-kernel-state.md) · [0003 eccezione `unsafe` nel benchmark](docs/adr/0003-bench-allocator-unsafe.md) · [0004 runtime Tokio](docs/adr/0004-tokio-runtime.md) · [0005 drain del sequencer (misurato, non adottato)](docs/adr/0005-sequencer-drain.md) · [0006 allocazioni e IPC](docs/adr/0006-allocations-ipc-path.md) · [0007 lookup/sort nel core](docs/adr/0007-core-lookup-sort.md) · [0008 liveness, takeover, ripresa](docs/adr/0008-liveness-takeover-reconnect.md) · [0009 memoria e sicurezza UDS](docs/adr/0009-memory-budget-uds-security.md) · [0010 ingresso fair (VS-003)](docs/adr/0010-fair-ingress.md)
- Benchmark: [VS-001](docs/benchmarks/vs001.md) · [VS-002 (confronti, campagne, analisi critica)](docs/benchmarks/vs002.md) · [VS-003 (politiche di ingresso)](docs/benchmarks/vs003.md)
- [Matrice di tracciabilità dei requisiti, limiti noti e valutazione critica](docs/traceability.md)

## Struttura

```text
Cargo.toml / Cargo.lock / rust-toolchain.toml   workspace, lockfile, toolchain fissata (1.97.0)
config/demo.toml                                configurazione statica della demo (sorgente 1001, tag 101-103)
config/profiles/micro.toml | server.toml        profili VS-002: current_thread / multi_thread(2), budget di memoria esplicito
proto/scada/driver/v1/driver.proto              protocollo kernel <-> driver (versionato)
proto/scada/introspect/v1/introspect.proto      introspezione read-only per test/benchmark
proto/baseline/scada-v1.0.binpb                 immagine Buf di riferimento per `buf breaking`
buf.yaml                                        regole buf lint (STANDARD) e breaking (FILE)
crates/scada-core/                              state machine pura + RTDB + digest (+ test unitari/property)
crates/scada-protocol/                          tipi generati + framing limitato (+ test)
crates/scada-runtime/                           binario scada-kernel: UDS, sessioni, ingresso, sequencer (+ test multiprocesso)
crates/scada-sched/                             VS-003: modello puro dello scheduler di ingresso + simulatore politiche
apps/scada-sim-driver/                          processo driver simulato
apps/scada-bench/                               benchmark riproducibili
fuzz/                                           workspace cargo-fuzz separato (nightly) + seed corpus
scripts/check.sh | check-proto.sh | bench.sh    gate completo, verifica schemi, benchmark VS-001
scripts/bench_campaign.py | bench_analyze.py    campagne A/B randomizzate (VS-002) e analisi (mediana, dispersione, permutazione)
scripts/micro_ab.py | criterion_table.py        microbenchmark Criterion interlacciati
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
cargo +nightly fuzz run -O decode_equivalence corpus/decode_equivalence -- -max_total_time=300   # VS-002 C3
```

Test runtime con lo scheduler multi-thread (la CI esegue entrambi): `cargo build --release -p scada-sim-driver && SCADA_TEST_SCHEDULER=multi_thread:2 cargo test --release -p scada-runtime`.

Test runtime sotto una politica di ingresso VS-003 (la CI esegue `source_fair:4`, `source_fair:1`, `source_capped:2`): `SCADA_TEST_INGRESS=source_fair:4 cargo test --release -p scada-runtime`. Modello dello scheduler: `cargo test -p scada-sched`; simulatore delle politiche: `cargo run --release -p scada-sched --example policy_model`.

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
| cargo-fuzz | 0.13.2 |
| protoc | non usato (compilatore `protox` 0.10.0, `prost-build` 0.14.4) |
| Buf | 1.75.0 |
| Benchmark | harness proprio `scada-bench` (nessun Criterion: servono percentili p99.9 esatti e conteggio allocazioni) |
| OS | Linux 6.18.44 x86_64 (VM KVM, 4 vCPU Intel Xeon @ 2.80GHz, 15 GiB) |
