# moscascada — SCADA Engine, vertical slice VS-001

Prima vertical slice del percorso `driver esterno → IPC → validazione → sequencer → state machine deterministica → RTDB in RAM`, in Rust stable, Linux-first.

**Stato:** VS-001 implementata e verificata su Linux x86_64. ARM64: compilazione verificata (`cargo check --target aarch64-unknown-linux-gnu`), **link ed esecuzione su hardware ARM64 non testati**. Lo stato è solo in RAM: nessun ACK è durevole.

Documenti:
- [Contratto semantico](docs/contract.md) · [Architettura](docs/architecture.md)
- ADR: [0001 IPC](docs/adr/0001-ipc.md) · [0002 stato del kernel e digest](docs/adr/0002-kernel-state.md) · [0003 eccezione `unsafe` nel benchmark](docs/adr/0003-bench-allocator-unsafe.md)
- [Benchmark e metodologia](docs/benchmarks/vs001.md)
- [Matrice di tracciabilità dei requisiti, limiti noti e valutazione critica](docs/traceability.md)

## Struttura

```text
Cargo.toml / Cargo.lock / rust-toolchain.toml   workspace, lockfile, toolchain fissata (1.97.0)
config/demo.toml                                configurazione statica della demo (sorgente 1001, tag 101-103)
proto/scada/driver/v1/driver.proto              protocollo kernel <-> driver (versionato)
proto/scada/introspect/v1/introspect.proto      introspezione read-only per test/benchmark
proto/baseline/scada-v1.0.binpb                 immagine Buf di riferimento per `buf breaking`
buf.yaml                                        regole buf lint (STANDARD) e breaking (FILE)
crates/scada-core/                              state machine pura + RTDB + digest (+ test unitari/property)
crates/scada-protocol/                          tipi generati + framing limitato (+ test)
crates/scada-runtime/                           binario scada-kernel: UDS, sessioni, sequencer (+ test multiprocesso)
apps/scada-sim-driver/                          processo driver simulato
apps/scada-bench/                               benchmark riproducibili
fuzz/                                           workspace cargo-fuzz separato (nightly) + seed corpus
scripts/check.sh | check-proto.sh | bench.sh    gate completo, verifica schemi, benchmark
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
```

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
             [--throttle-sequencer-us N]   # solo test di saturazione: ritardo artificiale nel sequencer
             [--check-config]              # valida la configurazione ed esce
```

Il kernel termina con `SIGTERM`/`SIGINT`; lo stato volatile viene scartato.

## Benchmark

```bash
./scripts/bench.sh       # scrive docs/benchmarks/results/<host>-<data>.md
```

Metodologia e risultati misurati: [docs/benchmarks/vs001.md](docs/benchmarks/vs001.md).

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
