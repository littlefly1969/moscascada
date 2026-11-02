# EXP-001 — risultati grezzi (open-wal vs SQLite)

Report e conclusione: [`docs/experiments/exp-001-report.md`](../../../experiments/exp-001-report.md).

| File | Contenuto |
|---|---|
| `raw/<fase>-<cella>.txt` | output **verbatim** di ogni esecuzione di `scada-bench exp001` (comando, exit, durata, righe `kind=...`) |
| `raw/fsync-probe.txt` | latenza `fdatasync` del filesystem usato |
| `exp001-*.csv` | stesse righe in CSV (una riga per `kind=exp001`, `-query`, `-recovery`, `-sample`) |
| `summary.md` | mediana [min–max] per cella e variante (`scripts/exp001_summary.py`) |
| `environment.txt` | hardware, filesystem, mount, toolchain, versioni delle crate (una sezione per avvio della campagna) |
| `cargo-deny.log` | `cargo deny check` con tutte le feature (include open-wal) |
| `tests.log` | `cargo test --release -p scada-journal --features experimental-open-wal -- --nocapture` |

Riprodurre (stessa macchina per tutte le varianti; mai tmpfs per i numeri):

```sh
cargo build --release -p scada-bench --features experimental-open-wal
python3 scripts/exp001_campaign.py --store /percorso/su/disco/reale --out docs/benchmarks/results/exp001 \
    --phases fsync,batch,delay,shape,read,large --reps 10
python3 scripts/exp001_campaign.py --store /percorso/su/disco/reale --out docs/benchmarks/results/exp001 \
    --phases latency,soak --reps 10 --soak-min 30
python3 scripts/exp001_summary.py docs/benchmarks/results/exp001/*.csv > docs/benchmarks/results/exp001/summary.md
```
