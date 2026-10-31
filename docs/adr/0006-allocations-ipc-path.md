# ADR 0006 — Allocazioni ripetute e percorso IPC (C2, C3, C5)

- **Stato:** accettato (VS-002)
- **Data:** 2026-10-31
- **Ambito:** `scada-runtime::server` (reader, writer), `scada-runtime::convert`. Wire protocol invariato.

## Contesto

La profilazione di VS-001 (`--diag`, `perf`, `docs/benchmarks/vs002.md` §3) mostrava che con client pipelined il collo di bottiglia **non** è `Kernel::apply` (pochi µs per batch da 128 osservazioni, sul thread sequencer al ~30 % di CPU) ma il thread del runtime Tokio `current_thread` al ~97 %: per ogni batch una `read` dell'header e una del payload, un `DriverMessage` nuovo (allocazione del `Vec` di osservazioni), un `InputAck` nuovo (copia della `String` `kernel_instance_id`), una `write` per ACK. Il brief (§5 C2, C3, C5) chiede di eliminare allocazioni e syscall evitabili senza cambiare il contratto e senza `unsafe`, mmap o compressione.

## Varianti misurate

Campagna E2E 1 (`docs/benchmarks/results/vs002/e2e/c1-steps.txt`): 8 varianti × 5 celle × 7 run in ordine casuale, 3 s + 1 s di warmup per run, stesso binario client per tutte. Mediane, [min–max], test di permutazione; ** = p < 0,05 e |Δ| oltre la semi-dispersione della baseline. Valori completi in `c1-steps-vs-vs001.md`.

| variante | modifica cumulativa |
|---|---|
| `vs001` | binario VS-001 (b17413f) |
| `s1` | VS-002 strumentazione + switch scheduler, `current_thread` (nessuna ottimizzazione) |
| `c2` | + un solo `InputAck` riusato per connessione (nessuna copia dell'instance id per ACK) |
| `c3` | + decode del frame dati in un `ObservationBatch` riusato (fast path), fallback generico |
| `c5` | + indice denso nel core (C4, ADR 0007); switch C5 disattivati |
| `c5rb` | + `read_buffer_bytes = 8192` |
| `c5rbco` | + `ack_coalesce_max = 32` |

Throughput (batch/s), mediane:

| cella | vs001 | c2 | c3 | c5 | c5rb | c5rbco |
|---|---:|---:|---:|---:|---:|---:|
| 1 client, 1 obs, stop-and-wait | 8661 | 8282 ~ | 8715 ~ | 8412 ~ | 8870 ~ | 8404 ~ |
| 8 client × 128 obs, stop-and-wait | 17750 | 17033 ~ | 17576 ~ | 18280 ~ | 18739 ~ | 18408 ~ |
| 1 client × 128 obs, pipeline 32 | 31159 | 33685 ~ | **45426 (+46 %)** | **36977** | **82172 (+164 %)** | **69847 (+124 %)** |
| 8 client × 1 obs, pipeline 32 | 35719 | 38498 ~ | 40231 ~ | 35063 ~ | **163307 (+357 %)** | **231601 (+548 %)** |
| 8 client × 128 obs, pipeline 32 | 30038 | 30071 ~ | **37389 (+25 %)** | **38707 (+29 %)** | **73867 (+146 %)** | **83680 (+179 %)** |

(~ = differenza non significativa rispetto a `vs001`.) p99 nelle celle pipelined: −41 %…−70 % con `c5rb`/`c5rbco`; nelle celle stop-and-wait nessuna differenza oltre il rumore.

## Decisioni

### C2 — ACK riusato: **adottato** (beneficio di allocazione, non di throughput)

Nessun effetto E2E oltre il rumore. Il beneficio è strutturale e misurabile in allocazioni: un `InputAck` senza la copia della `String` per ACK (`set_ack`, test `reused_ack_encodes_identically` verifica che i byte prodotti siano identici a quelli di un `InputAck` nuovo). Il codice aggiuntivo è una funzione di 10 righe. Allocazioni per batch: `docs/benchmarks/vs002.md` §6 (dhat).

### C3 — decode in batch riusato: **adottato**

Ripetibile: +46 % (1×128 pipelined), +25 % (8×128 pipelined), p50 −21…−36 %; microbench `decode/fresh` vs `decode/reuse` in `docs/benchmarks/results/vs002/micro/`. Il fast path si attiva solo se il frame è **esattamente** un campo `batch` (tag 0x12 + varint di lunghezza pari al resto); qualsiasi altra codifica valida (campi ripetuti, ordine diverso, campi sconosciuti) passa dal decoder generico. Equivalenza: proptest in `convert.rs` e fuzz target `decode_equivalence` (stesso esito e stesso messaggio del decoder generico, con scratch sporco riusato fra input). Lo scratch viene azzerato con `Message::clear` (conserva la capacità).

### C5a — buffer di lettura per connessione: **adottato, default 8192 byte**

Il guadagno maggiore della slice: con frame pipelined una `recv` porta più frame, eliminando 2 syscall per batch. +164…+357 % di throughput e −41…−70 % di p99 nelle celle pipelined; nessun effetto in stop-and-wait (ogni frame arriva da solo). Costo: 8 KiB per connessione (incluso nel bound E1). Il controllo di `max_frame_bytes` resta sull'header, prima di ogni allocazione del payload; testato con e senza buffer (`malformed_input_with_read_buffer`, `malformed_input_unbuffered`). `0` = comportamento VS-001.

### C5b — più ACK già pronti per `write`: **adottato, default 32**

Rispetto a `c5rb`: +42 % ripetibile con batch piccoli (8 client × 1 oss.), p50 −38 %; altrove entro il rumore (1×128: −15 %, p = 0,27; 8×128: +13 %, p = 0,17). Nessun peggioramento ripetibile di p99/p99.9. Il writer **non attende mai** per riempire un gruppo: dopo aver ottenuto l'ACK in testa raccoglie solo quelli già risolti (`try_recv` sui `oneshot` successivi, in ordine), fino a `ack_coalesce_max`; l'ordine degli ACK resta quello dei batch e nessun ACK è anticipato. `1` = un ACK per `write` (VS-001). Effetto sulla sorgente rara: campagna 3.

## Alternative non adottate

- `writev`/`write_vectored` di header e payload: equivalente alla codifica in un buffer contiguo già riusato, non misurato perché non c'è un secondo buffer da evitare.
- Decodifica zero-copy (`bytes::Bytes` nei messaggi `prost`): i campi del batch sono scalari, non c'è nulla da condividere; il brief vieta di dichiararlo più veloce senza misura.
- mmap / shared memory, compressione: esclusi dal brief senza prova; il collo di bottiglia misurato erano syscall e wakeup, non la copia dei byte.
