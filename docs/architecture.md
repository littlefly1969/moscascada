# Architettura VS-001

```text
 processo scada-sim-driver                 processo scada-kernel (scada-runtime)
 ┌──────────────────────────┐   UDS stream  ┌──────────────────────────────────────────────────────────┐
 │ std::os::unix UnixStream │ ── frame ───▶ │ tokio current_thread (solo I/O)                          │
 │ scada-protocol (prost)   │ ◀── ACK ───── │  accept ─ semaforo max_driver_connections                │
 └──────────────────────────┘               │  sessione: handshake (timeout) → lettura frame limitati │
                                            │    → convert (proto → Operation)                         │
                                            │    → coda ACK per connessione (max_inflight)             │
                                            │    → mpsc limitata (input_queue_capacity) ──┐            │
                                            │                                              ▼            │
                                            │  thread "scada-sequencer" (unico writer)                  │
                                            │    assegna operation_sequence / link_generation           │
                                            │    Kernel::apply(SequencedInput)  ← scada-core (puro)     │
                                            │    risposta oneshot → ACK solo dopo il ritorno di apply   │
                                            │                                                           │
                                            │  socket introspezione (opzionale, sola lettura)           │
                                            └──────────────────────────────────────────────────────────┘
```

## Crate

| Crate | Ruolo | Dipendenze runtime |
|---|---|---|
| `scada-core` | Modello di configurazione validata, RTDB in RAM, `Kernel::apply`, snapshot, digest. Puro e sincrono. | `blake3` (feature `pure`) |
| `scada-protocol` | Tipi generati da `proto/` (`prost`, compilati con `protox`), framing limitato. | `prost`, `bytes` |
| `scada-runtime` | Lib + binario `scada-kernel`: config TOML, socket, sessioni, sequencer, introspezione. | core, protocol, `tokio`, `serde`, `toml` |
| `scada-sim-driver` | Processo driver simulato + piccolo client bloccante riusato da test e benchmark. | protocol |
| `scada-bench` | Benchmark riproducibili (non distribuito). | core, protocol, sim-driver |
| `fuzz/` | Workspace separato per `cargo-fuzz` (nightly). | core, protocol, runtime, `libfuzzer-sys` |

Il kernel non dipende dal simulatore né lo carica: aggiungere o cambiare un driver non richiede di ricompilare il kernel (solo lo schema è condiviso).

## Percorso di un batch

1. La sessione legge l'header (4 byte); valida la lunghezza contro `max_frame_bytes` **prima** di ridimensionare il buffer riusato della sessione; legge il payload entro `frame_timeout_ms`.
2. Decodifica `DriverMessage`; solo `ObservationBatch` è ammesso dopo il `Welcome`.
3. `convert::batch_from_wire` rifiuta modalità sconosciute e valori mancanti (nessun effetto sullo stato) e costruisce `scada_core::ObservationBatch`.
4. La sessione prenota uno slot nella propria coda ACK (limite `max_inflight_per_connection`), poi invia la richiesta alla coda del sequencer (limite `input_queue_capacity`). Se una delle due è piena la sessione smette di leggere dal socket: la backpressure arriva al `write` del produttore tramite il buffer del socket.
5. Il thread sequencer estrae le richieste in ordine FIFO (ordine di ammissione), propone `operation_sequence = last + 1` e chiama `Kernel::apply`.
6. L'esito torna alla sessione via `oneshot`; il task di scrittura invia gli `InputAck` nell'ordine dei batch.

## Thread e memoria

- 2 thread nel kernel: runtime Tokio `current_thread` e sequencer. Nessun thread per connessione.
- Ogni struttura ha un limite configurato: connessioni (semaforo), buffer di frame (≤ `max_frame_bytes` per sessione), ACK in volo per connessione, coda del sequencer, osservazioni per batch, canali per sorgente (≤ limite del `FullState`), stato di deduplicazione (1 batch per sorgente). Le connessioni rifiutate per limite non creano task.
- Il core alloca tutto in `Kernel::new` e non cresce durante `apply` (test `apply_never_grows_internal_storage`; benchmark: 0 allocazioni per `apply`).

## Guasti

| Guasto | Effetto |
|---|---|
| Driver ucciso (SIGKILL) | EOF sul socket → `SourceDisconnected` della generazione attiva → tag `Uncertain(SourceUnavailable)` con valore preservato. Il kernel continua. |
| Driver che invia frame invalidi | `ProtocolError` e chiusura della sola connessione; slot liberato. |
| Driver muto | `HANDSHAKE_TIMEOUT` o `FRAME_TIMEOUT`, chiusura. |
| Flood di connessioni | oltre `max_driver_connections`: rifiuto `TOO_MANY_CONNECTIONS` e chiusura immediata (best effort: se il client ha già scritto dati il kernel Linux può segnalargli `ECONNRESET`). |
| Kernel riavviato | Nuovo `kernel_instance_id`, tutti i tag `Unknown`, nessuna baseline: `full_state_required = true`. |
