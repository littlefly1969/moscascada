# Playbook operativo VS-007 — driver Modbus TCP (sola lettura)

Per chi installa e gestisce `scada-driver-modbus-tcp` (ADR 0015). Ogni procedura dice **cosa è certo**, **cosa non lo è** e **cosa fare**. Principio:

> un valore Modbus è un campionamento del driver, non un evento del dispositivo. Il driver conosce solo l'istante in cui ha **letto** (`acquired_at`, `clock_quality = Unknown`); non sa cosa è successo fra due letture, né quando il dispositivo ha cambiato valore.

Strumenti:

| Comando | Uso | Effetto |
|---|---|---|
| `scada-driver-modbus-tcp --config D.toml --check-config [--kernel-config K.toml]` | valida schema, catalogo, mapping, piano di lettura; con `--kernel-config` confronta sorgenti, canali, tipi e `max_observations_per_batch` del kernel. Stampa le richieste `READ fc=.. address=.. quantity=..` di ogni dispositivo | **nessun socket aperto**, nessuna connessione a dispositivi o kernel; exit 0 = valido, 2 = non valido |
| `scada-driver-modbus-tcp --config D.toml` | esecuzione: `READY`, poi righe `STATUS` (una per dispositivo) e `TRANSITION` su stdout | legge dai dispositivi, scrive solo verso il socket del kernel |
| `health_file` (opzionale) | JSON sostituito atomicamente a ogni `status_interval_ms` | — |
| `scada-modbus-sim` | simulatore su loopback per prove e soak | **mai** in impianto |

---

## 1. Sicurezza (prima dell'installazione)

Modbus TCP **non autentica e non cifra**: chiunque raggiunga la porta 502 di un dispositivo può leggerlo e, con le funzioni di scrittura, comandarlo. Il driver MoScada usa **solo FC01–FC04** (letture) e non contiene codice di scrittura, ma non protegge il dispositivo da altri client.

Obblighi:
1. **Rete OT segmentata**: i dispositivi stanno su una VLAN/segmento dedicato; nessun instradamento da reti office o Internet.
2. **Firewall** sull'host MoScada e sul segmento: in uscita solo `host MoScada → IP dispositivi:502/tcp`; in ingresso nulla. Il driver **non ascolta** su nessuna porta.
3. **Allowlist di dispositivi e unit id**: solo gli endpoint configurati; nessuna scansione o discovery (il driver non ne fa, e non vanno fatte con altri strumenti durante l'esercizio).
4. **Utente di servizio dedicato** senza privilegi; il socket del kernel (`[ipc] socket_path`) è un UDS locale con permessi limitati all'utente del driver.
5. **Gateway TCP/RTU**: l'`unit_id` seleziona lo slave seriale; un `unit_id` sbagliato può leggere un altro dispositivo con la stessa mappa. Verificare sul campo con una lettura nota.
6. Modbus Security (TLS, porta 802) e autenticazione: **non supportati** in VS-007, rinviati a una slice dedicata.

## 2. Configurazione e catalogo

- **Indirizzi zero-based.** Il catalogo usa indirizzi PDU. Una tabella del costruttore che scrive `40001` o `30001` indica l'indirizzo PDU **0** dell'area holding/input. La conversione si fa **una volta, a mano, nel file di profilo**; il driver non aggiunge né sottrae offset. Errore tipico: tutti i valori «sfasati di uno».
- **Profilo prodotto versionato** (`docs/driver-catalog/*.vN.toml`, schema `moscada.driver-catalog/1`): `id` + `version`. Una correzione della mappa = **nuova versione** (nuovo file), non modifica in place; i dispositivi passano alla nuova versione cambiando `profile_version`. Ogni profilo dichiara il documento sorgente (`source_document`).
- **Blocchi leggibili**: il planner legge solo dentro `[[blocks]]` e non unisce mai due blocchi. Se un dispositivo risponde con eccezione 02 (indirizzo illegale) a una lettura del piano, il blocco dichiarato è sbagliato: correggere il profilo, non il driver. `max_per_request` limita la quantità per richiesta per dispositivi che non accettano il massimo di protocollo (125 registri / 2000 bit).
- **Tipi e conversioni**: `bit`, `register_bit`, `u16`, `i16`, `u32`, `i32`, `f32`; ordine delle word e swap dei byte espliciti; scala razionale `numerator/denominator + offset` con `rounding` `exact`/`half_even`/`truncate`; `f32` solo verso i64 a virgola fissa con rounding esplicito, NaN/Inf rifiutati. `--check-config` rifiuta scale che possono andare in overflow su qualunque valore grezzo.
- **Validare sempre** con `--check-config --kernel-config` prima di ogni modifica in esercizio; il comando non apre socket ed è sicuro su un host di produzione.

## 3. Lettura delle righe `STATUS`

Campi principali (nomi stabili):

| Campo | Significato | Azione se anomalo |
|---|---|---|
| `device_state` | `starting` / `up` (ultimo scan completo) / `down` (ultimo tentativo fallito, retry con backoff) | §4 |
| `link` | sessione verso il kernel: `disconnected`, `connecting`, `handshaking`, `awaiting_full_state`, `streaming`, `backpressured`, `recovering` | §5–§6 |
| `durable` | `true` se il kernel concede le receipt durevoli (journal attivo, minor 1.2) | se `false` con journal atteso: verificare `[persistence]` del kernel |
| `scans_ok` / `scans_failed` / `scan_errors` | scansioni complete / fallite, per tipo (`connect`, `timeout`, `timeout_partial_frame`, `closed`, `bad_header`, `transaction_mismatch`, `unit_mismatch`, `exception`, `incoherent_response`, `decode`, …) | `exception` ripetute = profilo errato; `timeout` = rete o dispositivo lento |
| `overruns` | scansioni partite in ritardo rispetto a `scan_interval_ms` | ridurre richieste o aumentare l'intervallo (§8) |
| `spread_us` / `max_spread_us` | durata dalla prima all'ultima risposta di una scansione: i valori **non** sono simultanei entro questo intervallo | — |
| `pending` / `inflight` / `pending_high_water` | batch non ancora confermati (bound `max_pending_batches`) | prossimo al bound = kernel lento (§6) |
| `deferred` | scansioni non pubblicate perché il buffer era pieno; **contate, mai silenziose** | §6 |
| `rejected_retryable` / `rejected_fatal` | rifiuti tipizzati del kernel (es. `JOURNAL_QUEUE_FULL`) / definitivi | fatal ≠ 0: errore di configurazione, vedere `kernel_error` |
| `lost_kernel_restart` | batch non confermati durevoli persi in un riavvio del kernel, sostituiti da un FullState | atteso solo con kernel volatile o crash prima del commit |
| `ack_p50_us` … `ack_max_us` | latenza pubblicazione→ACK (su link durevole include commit+fsync del journal) | crescita = disco lento |
| `fatal` | errore di configurazione scoperto a runtime: il dispositivo è fermo | correggere e riavviare il driver |

## 4. Dispositivo non raggiungibile, eccezioni, timeout

**Cosa fa il driver:** al primo errore di una scansione **non pubblica nulla di quella scansione** (mai un FullState parziale), chiude la sessione di quel dispositivo verso il kernel e ritenta con backoff esponenziale (`device_backoff_min_ms`…`max_ms`). Il kernel vede `SourceDisconnected`: i tag della sorgente diventano `Uncertain(SourceUnavailable)`, gli allarmi su quei tag diventano indeterminati e **non rientrano** falsamente. Al recupero: nuova sessione, FullState completo, tag di nuovo `Good`.

**Certo:** nessun valore della scansione fallita è stato pubblicato; l'ultimo valore resta visibile ma degradato.
**Non certo:** cosa ha fatto il dispositivo durante il fermo. Un allarme scattato e rientrato fra due letture, o durante il fermo, non è visto (limite del polling).
**Isolamento:** un thread e una sessione per dispositivo; un dispositivo guasto non rallenta gli altri (test `one_failing_device_does_not_disturb_another`).

## 5. Kernel riavviato o fermo

- Il driver resta vivo e riprova la connessione con backoff (`kernel_reconnect_*`). Durante il fermo **non interroga i dispositivi** (contato in `unpublished_no_kernel`): al ritorno serve comunque un FullState.
- **Kernel con journal (VS-006)**: dopo il riavvio i valori recuperati sono `RecoveredUnverified` finché il driver non invia il FullState (primo scan dopo la riconnessione). Batch già confermati `APPLIED_DURABLE` sono nel journal; quelli non confermati oltre la sequenza recuperata sono contati in `lost_kernel_restart` e sostituiti dal FullState.
- **Kernel volatile**: tutto lo stato si riparte dal FullState.

## 6. Backpressure (journal lento, coda piena, outbox allarmi pieno)

Il kernel rifiuta con codici tipizzati (`JOURNAL_QUEUE_FULL`, `OUTBOX_CAPACITY_EXCEEDED`, `DISK_FULL`, …). Il driver ritrasmette **la stessa `source_sequence`** dopo un backoff (`retry_*`), in ordine; i batch successivi già inviati sono ritrasmessi dopo. Finché il buffer (`max_pending_batches`) non è pieno nulla si perde; a buffer pieno le nuove scansioni sono **rinviate e contate** (`deferred`), e la successiva pubblicata porta lo stato corrente (Delta rispetto allo stato che il kernel avrà dopo i batch già in coda), quindi l'RTDB converge ma i valori intermedi non pubblicati sono persi — è campionamento, non storico.

Azione: verificare disco e journal del kernel (playbook VS-006), poi la dimensione (§8).

## 7. Driver crash o riavvio

Nuovo processo = nuovo `source_epoch` casuale. Il driver **non ha spool durevole**: i batch non ancora confermati al momento del crash sono persi e il kernel marca la storia della sorgente `Unknown` per l'intervallo. Il nuovo processo parte con un FullState (riconciliazione live). Un driver polling **non** può dichiarare continuità `Complete`.

Avvio con systemd consigliato con `Restart=on-failure` e `RestartSec=2s`; SIGTERM chiude subito (il kernel vede le sessioni chiudersi).

## 8. Dimensionamento

Il budget non si inventa: si misura sul dispositivo reale.

1. Misurare il tempo di risposta del dispositivo `t_r` (p99) per una richiesta del piano (`--check-config` elenca le richieste; `ack_*` e `spread_us` danno indicazioni in esercizio).
2. Una scansione con `R` richieste dura circa `R × (t_r + inter_request_delay)`; deve stare **sotto** `scan_interval_ms` con margine (consigliato ≤ 50 %), altrimenti `overruns` cresce.
3. Molti dispositivi Modbus accettano **una sola connessione** o poche; il driver ne apre una per dispositivo e invia una richiesta alla volta.
4. Lato kernel il carico è `osservazioni cambiate/s`, non registri letti: un Delta contiene solo i valori cambiati. Con il journal, il limite è il writer (vedi `docs/benchmarks/vs007.md` §4 e il modello di capacità).
5. Gateway TCP/RTU: il bus seriale serializza tutti gli slave; a 19 200 baud una lettura di 10 registri costa ~15–20 ms. Sommare per tutti i dispositivi dietro lo stesso gateway.

## 9. Nuovo prodotto

1. Copiare `docs/driver-catalog/generic-io.v1.toml` in `<produttore>-<modello>.v1.toml`.
2. Compilare `[profile]` con il documento del costruttore e la sua revisione in `source_document`.
3. Dichiarare i blocchi **effettivamente leggibili** (dalla documentazione; in caso di dubbio blocchi piccoli).
4. Convertire gli indirizzi a zero-based (§2), tipi, ordine word, scale.
5. `--check-config` e una prova su banco con il dispositivo reale, confrontando i valori con il pannello del dispositivo. **Senza prova su banco il profilo è da considerare non verificato.**

## 10. Limiti noti (VS-007)

- Sola lettura (FC01–FC04); nessuna scrittura, nessun FC05/06/15/16, nessun FC23.
- Nessun timestamp del dispositivo; solo `acquired_at` del driver, `clock_quality = Unknown`.
- Nessuno spool durevole nel driver; continuità mai `Complete`.
- Una richiesta per connessione alla volta; nessun pipelining.
- Modbus RTU seriale diretto, Modbus Security/TLS, discovery: non supportati.
- Nessun test su hardware Modbus reale né su ARM64 reale; prove solo con il simulatore su loopback.
