# ADR 0004 — Runtime Tokio del bordo I/O: `current_thread` vs `multi_thread`

- **Stato:** accettato (VS-002)
- **Data:** 2026-10-31
- **Ambito:** `scada-runtime` (`[runtime] scheduler`, `worker_threads`), binario `scada-kernel`. Il core e il sequencer non cambiano.

## Contesto

VS-001 usava solo `current_thread`: tutte le sessioni (lettura del socket, decodifica, conversione, scrittura degli ACK) giravano su un thread. La diagnosi di VS-002 (`--diag`, CPU per thread) mostra quel thread al 95–99 % di CPU in tutte le celle con più client pipelined, mentre il thread sequencer (che esegue `Kernel::apply`) resta al 15–45 %: il collo di bottiglia era il bordo I/O, non il core. Il brief (§4) chiede ingestion multicore conservando un solo writer, l'ordine per sorgente e l'assenza di lock nel core.

Documentazione di riferimento: [modelli di runtime Tokio](https://docs.rs/tokio/latest/tokio/runtime/index.html) (il `current_thread` esegue tutti i task sul thread che chiama `block_on`; il `multi_thread` usa un pool con work stealing), [`runtime::Builder`](https://docs.rs/tokio/latest/tokio/runtime/struct.Builder.html) (`worker_threads`, `max_blocking_threads`, `thread_name`).

## Decisione

1. Lo scheduler è una scelta **esplicita** di configurazione: `[runtime] scheduler = "current_thread" | "multi_thread"`; con `multi_thread` `worker_threads` è **obbligatorio** (1..=64) e non viene mai derivato dal numero di CPU (brief §10: nessuna scelta hard-coded per una CPU). Override da riga di comando per i benchmark.
2. Default `current_thread` (profilo Micro, compatibile con VS-001: 2 thread). Profilo Server: `multi_thread` con `worker_threads = 2` come valore **misurato su questa VM**, non come regola.
3. `max_blocking_threads(1)`: il kernel non usa `spawn_blocking`; il limite evita un pool di 512 thread latente.
4. Il sequencer resta un thread OS dedicato (non un task Tokio) che possiede il `Kernel`; nessun `Mutex`, nessun stato condiviso fra worker: ogni sessione comunica col sequencer solo attraverso la coda `mpsc` limitata e una `oneshot` per batch.
5. Ordine: reader e writer di una sessione sono futures dello **stesso task** (`join!`), quindi una sessione non è mai eseguita in parallelo con se stessa; il parallelismo è fra sessioni. L'ordine di una sorgente dipende solo dal socket, dalla coda FIFO e dalla coda ACK FIFO. Test 3/4 del brief: `vs002.rs::ingestion_modes_preserve_order_and_give_the_same_normalized_state` (16 sorgenti con takeover, `current_thread`, `multi_thread(1/2/4)`, stesso stato normalizzato), tutta la suite di runtime eseguita anche con `SCADA_TEST_SCHEDULER=multi_thread:2` (CI).

## Misure (campagna 2, `docs/benchmarks/results/vs002/e2e/c2-sched.txt`)

5 varianti × 13 celle × 7 run in ordine casuale, 3 s per run; stessa build per `ct`, `mt1`, `mt2`, `mt4` (default VS-002: buffer di lettura 8 KiB, coalescing 32, drain 1). VM KVM a 4 vCPU **condivisa con i processi client** (fino a 16 thread client).

Throughput (batch/s, mediana di 7 run):

| cella | vs001 | ct | mt1 | mt2 | mt4 |
|---|---:|---:|---:|---:|---:|
| 1 client × 1 obs, pipeline 32 | 38243 | 199968 | 229968 | 144753 | 130028 |
| 1 client × 128 obs, pipeline 32 | 31789 | 71358 | 74631 | 49240 | 45353 |
| 2 × 1, pipeline 32 | 38828 | 197329 | 223983 | 290165 | 228834 |
| 2 × 128, pipeline 32 | 30539 | 69612 | 75562 | 100427 | 83958 |
| 4 × 1, pipeline 32 | 37993 | 217998 | 199941 | 421828 | 305750 |
| 4 × 128, pipeline 32 | 31354 | 83428 | 75655 | 138890 | 114617 |
| 8 × 1, pipeline 32 | 33975 | 213553 | 216564 | 436589 | 368973 |
| 8 × 32, pipeline 32 | 33485 | 137835 | 149049 | 248696 | 244143 |
| 8 × 128, pipeline 32 | 29095 | 67852 | 73415 | 125725 | 124569 |
| 16 × 1, pipeline 32 | 35724 | 211053 | 234813 | 408129 | 420571 |
| 16 × 128, pipeline 32 | 29457 | 63604 | 69354 | 121344 | 117150 |
| 8 × 128, stop-and-wait | 16714 | 19363 | 17866 | 40794 | 29971 |
| 16 × 1, stop-and-wait | 20764 | 20150 | 20650 | 56064 | 46595 |

p99 send→ACK (µs, mediana):

| cella | vs001 | ct | mt1 | mt2 | mt4 |
|---|---:|---:|---:|---:|---:|
| 1 × 1, pipeline 32 | 1530 | 384 | 342 | 497 | 575 |
| 1 × 128, pipeline 32 | 1797 | 1159 | 1130 | 1306 | 1392 |
| 4 × 1, pipeline 32 | 7420 | 3674 | 3530 | 832 | 1325 |
| 8 × 128, pipeline 32 | 19465 | 8892 | 8668 | 5315 | 6020 |
| 16 × 128, pipeline 32 | 38480 | 20382 | 17755 | 10761 | 13211 |
| 8 × 128, stop-and-wait | 2637 | 2491 | 1600 | 463 | 714 |
| 16 × 1, stop-and-wait | 1647 | 1467 | 1471 | 719 | 1118 |

Tabelle complete con dispersione, p-value, CPU per thread: `c2-sched-vs-ct.md`, `c2-sched-vs-vs001.md`.

## Analisi

- **Con ≥ 2 client `multi_thread(2)` raddoppia circa il throughput** rispetto a `current_thread` (+44…+178 %, tutte le differenze ripetibili) e riduce il p99 (−41…−81 % da 4 client in su). Il costo è CPU: +36…+63 % di CPU di processo (due worker attivi invece di uno).
- **Con 1 client `multi_thread` peggiora** (−28…−36 % di throughput, p99 più alto): la sessione è un solo task, quindi non c'è parallelismo da sfruttare, mentre i risvegli fra worker (risposta del sequencer → task della sessione su un altro worker, work stealing) aggiungono latenza. `mt1` ≈ `ct` (entro il rumore nella maggior parte delle celle): il runtime multi-thread con un worker non ha costi visibili, ma neanche benefici.
- **`mt4` non migliora su `mt2`** su questa VM: 4 worker + sequencer + main + i thread client si contendono 4 vCPU. Non va letto come "4 worker non servono": su un host con più core e client esterni il risultato può essere diverso, ed è per questo che `worker_threads` resta una scelta di configurazione.
- RSS: `current_thread` +0,5 MiB rispetto a VS-001 (codice di `rt-multi-thread` collegato, buffer C5), `mt2` +0,3 MiB su `ct`; thread 2 / 3 / 4 / 6.
- Effetto sulla sorgente rara (p99/p99.9 sotto carico): campagna 3, ADR 0005 e `docs/benchmarks/vs002.md` §5.

## Alternative considerate

- **Più runtime `current_thread`, uno per gruppo di connessioni** (sharding manuale): stesso effetto atteso di `multi_thread` senza work stealing, ma richiede un bilanciatore di accept proprio; non adottato senza una misura che mostri un vantaggio rispetto al pool di Tokio.
- **Thread per connessione (I/O bloccante)**: elimina il runtime ma lega i thread al numero di driver (fino a 1024) e richiede timeout per socket; escluso.
- **Affinità CPU, `SCHED_FIFO`, PREEMPT_RT**: esclusi dal brief in questa slice; nessun cambio di privilegio del processo. Il confine futuro è `[runtime]`.
