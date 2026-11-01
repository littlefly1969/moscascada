# Architettura (VS-001 + VS-002 + VS-003 + VS-004 + VS-005)

```text
 processi driver (invariati)               processo scada-kernel (scada-runtime)
 ┌──────────────────────────┐   UDS stream  ┌───────────────────────────────────────────────────────────────┐
 │ std::os::unix UnixStream │ ── frame ───▶ │ Tokio, solo bordo I/O: current_thread (default, Micro)        │
 │ scada-protocol (prost)   │ ◀── ACK ───── │   oppure multi_thread(N worker espliciti) (Server)            │
 └──────────────────────────┘               │  accept ─ semaforo max_driver_connections                     │
                                            │  sessione = 1 task (reader + writer uniti con join!):         │
                                            │    handshake (timeout) → frame limitati [buffer lettura C5]    │
                                            │    → decode in batch riusato (C3) → convert (proto → core)     │
                                            │    → slot ACK (max_inflight) → mpsc limitata (input_queue)  ─┐│
                                            │    writer: ACK in ordine, più ACK già pronti per write (C5),  ││
                                            │            ogni write con scadenza (D2)                        ││
                                            │                                                                ▼│
                                            │  thread "scada-sequencer" (unico writer, unico thread OS)       │
                                            │    blocking_recv: una richiesta per risveglio (C1 non adottato) │
                                            │    assegna operation_sequence / link_generation                 │
                                            │    Kernel::apply(SequencedInput)  ← scada-core (puro)           │
                                            │    risposta oneshot → ACK solo dopo il ritorno di apply         │
                                            │    takeover: segnala al link precedente di chiudersi (D3)       │
                                            │    [--capture-inputs: registra gli input sequenziati (D5)]      │
                                            │  socket introspezione (opzionale, 0600, 4 sessioni, idle timeout)│
                                            │  [--diag: istogrammi limitati per stadio, fuori dal core]       │
                                            │  [VS-004, solo se una sorgente è sorvegliata: thread            │
                                            │   "scada-watchdog" → flag tick + Tick di risveglio; heartbeat    │
                                            │   = contatore atomico per link, mai in coda]                   │
                                            │  [VS-005, solo con [alarms]: AlarmEngine dentro Kernel::apply;  │
                                            │   socket di controllo scada.alarm.v1 (allowlist uid, limiti);   │
                                            │   le richieste passano dal sequencer (corsia di controllo)]     │
                                            └───────────────────────────────────────────────────────────────┘
```

Il nucleo deterministico è invariato: `scada-core` resta puro (nessun I/O, clock, lock, thread, Tokio, Protobuf), `Kernel::apply` è l'unica funzione di transizione e un solo thread OS la chiama. VS-004 aggiunge solo operazioni sequenziate e marker di dimensione fissa al core; il tempo resta nel runtime. VS-002 cambia solo il bordo I/O, il trasporto interno fra sessioni e sequencer e due dettagli algoritmici del core che non cambiano alcun risultato (indice canali denso; vedi `tests/equivalence.rs`).

## Crate

| Crate | Ruolo | Dipendenze runtime |
|---|---|---|
| `scada-core` | Configurazione validata, RTDB in RAM, `Kernel::apply`, snapshot, digest. Puro e sincrono. | `blake3` (feature `pure`) |
| `scada-protocol` | Tipi generati da `proto/` (`prost`, compilati con `protox`), framing limitato. | `prost`, `bytes` |
| `scada-runtime` | Lib + binario `scada-kernel`: config TOML, socket, sessioni, ingresso (VS-003), sequencer, introspezione, diagnostica, budget di memoria, cattura input. | core, protocol, sched, `prost` (stessa versione, solo `decode_varint`), `tokio` (`rt`, `rt-multi-thread`, …), `serde`, `toml` |
| `scada-sched` | VS-003: modello puro dello scheduler di ingresso (`FairQueue`, RR/DRR) + simulatore delle politiche. Nessuna dipendenza. | — |
| `scada-sim-driver` | Driver simulato + client bloccante; procedura di ripresa `resume` (D1). | protocol |
| `scada-bench` | Benchmark E2E (`cell`, `mixed`, `saturation`, `e2e`) e microbenchmark Criterion (`benches/micro.rs`). Non distribuito. | core, protocol, sim-driver; dev: `criterion`, runtime |
| `fuzz/` | Workspace separato per `cargo-fuzz` (nightly). | core, protocol, runtime |

## Percorso di un batch

1. La sessione legge l'header (4 byte), valida la lunghezza contro `max_frame_bytes` **prima** di ridimensionare il buffer riusato, legge il payload entro `frame_timeout_ms`. Con `read_buffer_bytes > 0` le letture passano da un `BufReader` per connessione: una `recv` può portare più frame pipelined (C5).
2. `convert::decode_driver_message`: se il frame è esattamente un campo `batch` (il caso di ogni driver conforme) lo decodifica, con `prost`, in un `ObservationBatch` riusato della sessione (capacità conservata); ogni altra forma passa dal decoder generico (C3). Solo `ObservationBatch` è ammesso dopo il `Welcome`.
3. `convert::batch_from_wire` rifiuta modalità sconosciute e valori mancanti e costruisce lo `scada_core::ObservationBatch` (un `Vec` per batch, di capacità esatta).
4. La sessione prenota uno slot nella propria coda ACK (`max_inflight_per_connection`), poi invia la richiesta alla coda del sequencer (`input_queue_capacity`). Se una delle due è piena la sessione smette di leggere dal socket: la backpressure arriva al `write` del produttore.
5. Il thread sequencer preleva una richiesta per risveglio (`blocking_recv`) e la applica, **nell'ordine della coda**. Nessuna fusione, nessuna riscrittura di sequenze. Un drain limitato (`blocking_recv_many`) è stato misurato e rimosso: nessun vantaggio ripetibile e più attesa per la sorgente rara (ADR 0005).
6. L'esito torna alla sessione via `oneshot`; il writer invia gli `InputAck` nell'ordine dei batch. Se ACK successivi sono già disponibili (senza attendere) li codifica nella stessa `write` (fino a `ack_coalesce_max`, C5). Ogni `write` ha la scadenza `ack_write_timeout_ms` (D2).

## Ingresso verso il sequencer (VS-003, ADR 0010)

`scada-runtime::ingress` astrae il passo 4–5 del percorso: `Ingress::submit` (lato sessioni, cancel-safe) e `Inbox::blocking_recv` (lato sequencer). Due politiche esplicite (una terza, `source_capped`, è stata misurata e non tenuta: ADR 0010 §11) in `[ipc] ingress_policy`:

- `fifo` (default): la `mpsc` limitata di VS-002.
- `source_fair`: permesso della sorgente → permesso globale (`input_queue_capacity`) → `push` sotto `Mutex` in una `scada_sched::FairQueue` (una lane FIFO limitata per sorgente, anello delle lane non vuote con al più una presenza per lane, round-robin a una richiesta per turno, O(1)); il sequencer attende su `Condvar`. `Connect`/`Disconnect` entrano nella lane della propria sorgente come richieste di controllo (fuori dalla quota dati: la FIFO della lane è la barriera di disconnessione); l'introspezione ha una lane propria.

I permessi viaggiano dentro la richiesta e sono rilasciati quando il sequencer la scarta dopo `apply`; ordine di acquisizione fisso (slot ACK → sorgente → globale → lock), nessun ciclo. Se il sequencer termina, il suo lato chiude i semafori e scarta le richieste in coda: nessuna sessione resta sospesa e nessun ACK viene inventato. `scada-sched` è un crate puro (nessun Tokio, clock, I/O, lock), verificato contro un oracle con proptest; contiene anche il simulatore a eventi discreti usato per progettare le varianti.

## Supervisione di liveness e freschezza (VS-004, ADR 0011)

Opzionale e **disabilitata per default**: senza `liveness_timeout_ms` / `freshness_timeout_ms` in nessuna sorgente non esistono `Supervisor`, thread watchdog, tick né marker mantenuti, e digest/fingerprint restano v1.

- **Tempo solo nel runtime.** `scada-runtime::supervision` possiede il clock monotono. Il core riceve tre operazioni sequenziate senza tempo: `SourceSilenceExpired{source, gen}`, `SourceLivenessRestored{source, gen}`, `FreshnessExpired{source, gen, observed_up_to}`. Le guardie (generazione del link, token in operation sequence confrontato con il marker `observed` di ogni tag) sono verificabili dal core senza clock; quelle obsolete sono no-op.
- **Un solo watchdog.** Il thread `scada-watchdog` alza un tick a cadenza fissa (`watchdog_poll_ms`): flag atomico + al più una richiesta `Tick` nella corsia di controllo dell'ingresso (+1 di capacità) per svegliare il sequencer inattivo; mai bloccante. Il sequencer controlla il flag dopo ogni richiesta: sotto flood il tick aspetta al più un `apply`.
- **Al tick** (thread sequencer, un'unica lettura del clock): per ogni sorgente sorvegliata confronta `last_batch_operation` del kernel e il contatore heartbeat del link con quelli del tick precedente (evidenza datata con l'istante del tick, quindi mai in anticipo), decide silenzio / ripristino / scadenza e applica con lo stesso `apply` dei batch. La freschezza usa un anello preallocato `(istante, last_batch_operation)` per sorgente: il token è il valore più recente con età `>= T`.
- **Heartbeat** (protocollo 1.1): la sessione valida la sequenza e incrementa un `AtomicU64` del link. Nessuna richiesta al sequencer, nessun ACK, nessuna allocazione: un flood costa solo decodifica nella sessione.
- **Cattura/replay:** le tre operazioni sono righe `S`/`R`/`X` del file `--capture-inputs`; il replay non usa clock.

## Motore allarmi e Event Outbox (VS-005, ADR 0012)

Opzionale e **disabilitato per default**: senza `[alarms]` il campo `Kernel::alarms` è `None`, non esistono outbox, staging, socket di controllo né lavoro per tag, e digest/fingerprint restano v1/v2.

```text
 operazione sequenziata (batch, link, scadenza VS-004, ACK, watermark)
        │
        ▼  Kernel::apply  (un solo thread, nessuna allocazione)
   validazione VS-001..004 ──Err──▶ nulla cambia
        │
   preflight allarmi: per ogni tag il cui (valore, qualità) cambierebbe,
   regole del tag via CSR → nuovo stato in staging preallocato (worst fan-out)
        │
   check: staged ≤ free slot, event_seq/occurrence senza overflow ──Err──▶ nulla cambia
        │
   commit infallibile: RTDB → stati allarme → eventi contigui nel ring → indice active|unacked
        │
        ▼
   ring di C eventi (48 B)  ── ReadEvents / snapshot (pagine, cursori, gap) ──▶ HMI (non durevole)
        ▲
   AdvanceDurableWatermark (sequenziato; VS-006 journal, in VS-005 solo sink simulato di test)
```

- **Core** (`scada-core::alarm`): `AlarmTable` (regole compilate 24 B, ordinate per id, CSR tag → regole, worst fan-out per sorgente) costruita da `ValidatedConfig::validate_with_alarms`; `AlarmEngine` (stati 32 B per regola, bitset a due livelli degli allarmi `active || unacked`, ring prefillato, staging) creato da `Kernel::new`. Nessuna stringa nella parte dinamica; nomi solo nel table.
- **Runtime** (`scada-runtime::alarms`): `AlarmRuntime` del sequencer (contatori di rifiuto per origine, overload `Normal/High/Critical`, disconnessioni differite e ritentate dopo ogni rilascio), accept loop e sessioni del socket di controllo. ACK, snapshot, letture e capacità sono `Request::Alarm` nella corsia di controllo dell'ingresso: il sequencer risponde con lo stato al proprio `operation_sequence`, le pagine sono riempite in buffer riusati della sessione.
- **Backpressure**: un batch con transizioni che non entrano è `REJECTED/OUTBOX_CAPACITY_EXCEEDED` (nulla applicato); un `Hello` analogo riceve `HandshakeRejected`; disconnessioni e scadenze restano pendenti e sono ritentate. La saturazione è visibile in `GetCapacityStatus`, nell'introspezione (`AlarmSummary`) e su stderr, mai come evento nel ring pieno.
- **Cattura/replay**: ACK e watermark sono righe `A` / `W`; gli eventi si ricavano deterministicamente.
- **Catalogo operatore** fuori dal kernel: `docs/alarm-catalog.md`.

## Ordine e concorrenza

- **Per sorgente**: una connessione = una sessione = un task. Reader e writer sono futures dello stesso task (`tokio::join!`), quindi **non** girano in parallelo fra loro nemmeno con `multi_thread`; il parallelismo multicore è **fra sessioni diverse**. L'ordine dei batch di una sorgente è quello del socket; la coda ACK è FIFO; il sequencer applica in ordine FIFO; nessun componente riordina. Test: `vs002.rs::ingestion_modes_preserve_order_and_give_the_same_normalized_state` (16 sorgenti, `current_thread` e `multi_thread(1/2/4)`).
- **Fra sorgenti** (`fifo`; con `source_fair` è il round-robin fra lane, sezione precedente): l'ordine è quello di ammissione nella coda del sequencer, che dipende dallo scheduling dei task e **non** equivale all'ordine reale di acquisizione, né è riproducibile fra due processi. Ciò che è riproducibile è lo stream già sequenziato: catturato con `--capture-inputs` e riapplicato a un kernel nuovo dà lo stesso digest (`captured_streams_replay_to_the_live_digest_in_every_mode`).
- **Scadenze VS-004 rispetto ai batch** (qualunque politica di ingresso): il tick decide sullo stato del kernel già prodotto dai batch applicati prima; un batch servito dopo il tick è applicato dopo le operazioni del tick. Le richieste ancora in coda non entrano mai nella decisione. Un batch che osserva un tag dopo il calcolo del token ha marker maggiore e il tag non scade.
- `SourceDisconnected` di un link è sempre accodato dopo tutti i batch che quel link ha sottomesso (la sessione lo invia dopo la fine del reader e del writer). Un link superato (takeover) non muta lo stato: i suoi batch ricevono `LINK_SUPERSEDED`, la sua disconnessione è `Ignored(StaleLinkGeneration)`.

## Thread e memoria

| Profilo | Thread del processo | Note |
|---|---|---|
| `current_thread` | 2: main (runtime Tokio) + `scada-sequencer` | default, profilo Micro |
| `multi_thread(N)` | N+2: `scada-io` × N, main (accept + segnali, dentro `block_on`), `scada-sequencer` | `max_blocking_threads(1)`: il kernel non usa `spawn_blocking` |
| + supervisione VS-004 | +1: `scada-watchdog` | solo se almeno una sorgente è sorvegliata; ~`1000 / watchdog_poll_ms` risvegli al secondo |
| + allarmi VS-005 | +0 | il motore è nel thread sequencer; le sessioni del socket di controllo sono task del runtime |

Ogni struttura ha un limite configurato; il **bound configurativo** delle allocazioni applicative è calcolato all'avvio in `u128` (`budget.rs`), stampato da `--check-config` e su stderr, e confrontato con `[runtime] memory_budget_bytes` se presente (avvio rifiutato se superato). Non è un bound dell'RSS (stack, allocatore, Tokio, libc, buffer socket del kernel Linux non sono inclusi): vedi ADR 0009.

## Guasti

| Guasto | Effetto |
|---|---|
| Driver ucciso (SIGKILL) | EOF → `SourceDisconnected` della generazione attiva → tag `Uncertain(SourceUnavailable)` con valore preservato. Il kernel continua. |
| Driver che invia frame invalidi | `ProtocolError` e chiusura della sola connessione; slot liberato. |
| Driver muto | `HANDSHAKE_TIMEOUT` o `FRAME_TIMEOUT` a metà frame, chiusura. Fra un frame e l'altro il link resta aperto: senza supervisione i tag restano `Good` (limite VS-001..003). |
| Driver muto con socket aperto, sorgente con `liveness_timeout_ms` (VS-004) | `SourceSilenceExpired` → sorgente `Silent`, tag `Uncertain(SourceSilent)` con valore preservato, link ancora `Connected`. Un heartbeat ripristina la comunicazione, non la qualità. |
| Driver che aggiorna solo alcuni canali, sorgente con `freshness_timeout_ms` (VS-004) | i tag non osservati da `>= T` passano a `Uncertain(StaleData)`, gli altri restano `Good`. |
| Flood di heartbeat (VS-004) | coalescenti nel contatore del link; nessuna coda, op seq o memoria in più; le altre sorgenti sono servite (`heartbeat_flood_is_bounded_and_does_not_starve`). |
| Driver che non legge gli ACK (VS-002) | dopo `ack_write_timeout_ms` la connessione è chiusa (`ack_write_timeouts`); gli input già applicati restano applicati; nessun ACK inventato; il driver riconcilia con reconnect (`last_accepted_source_sequence`). |
| Takeover con vecchio link inattivo (VS-002) | il sequencer segnala il vecchio link al `Welcome` del nuovo: `ProtocolError LINK_SUPERSEDED` e chiusura, slot liberato (`links_superseded`). |
| Flood di connessioni | oltre `max_driver_connections`: `TOO_MANY_CONNECTIONS` e chiusura immediata. |
| Thread sequencer terminato (panic) | il processo termina con errore (`sequencer thread stopped unexpectedly`) invece di accettare input non applicabili (test `sequencer_failure_stops_the_host`). |
| Kernel riavviato | Nuovo `kernel_instance_id`, tutti i tag `Unknown`, `full_state_required = true`. Con allarmi (VS-005): stati, eventi e ACK persi; richieste con il vecchio instance id ⇒ `INSTANCE_MISMATCH`. |
| Outbox allarmi pieno (VS-005) | batch con transizioni rifiutati (`OUTBOX_CAPACITY_EXCEEDED`), disconnessioni differite, ACK rifiutati; stato `CRITICAL` interrogabile; batch senza transizioni applicati. Senza journal (VS-006) la condizione dura fino al riavvio. |
| Client HMI ostile sul socket allarmi (VS-005) | uid non autorizzato ⇒ `UNAUTHORIZED` e chiusura; frame > 1024 B, protobuf malformato, versione errata ⇒ errore e chiusura; oltre `max_control_connections` ⇒ `TOO_MANY_CONNECTIONS`; l'actor dell'ACK è sempre l'uid del peer. |
