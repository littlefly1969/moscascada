# ADR 0014 — Time Authority, envelope temporale e provenance dei timestamp

- **Stato:** **proposto per review indipendente** (VS-006). Scritto prima dell'implementazione; estende ADR 0012 D12.
- **Data:** 2026-11-01
- **Ambito:** `scada-runtime::time_authority` (nuovo), `scada-journal` (colonne e tabelle temporali), `scada.driver.v1` minor 1.2 (sidecar timestamp **opzionali**), `scada.history.v1` (`GetTimeStatus`, campi temporali delle righe storiche).
- **Compagno:** ADR 0013.

## 1. Principi

1. **Il tempo non è causale.** L'ordine di storico ed effetti è `(timeline_id, operation_seq, event_seq / ordinal)`, **mai** un `ORDER BY` su un timestamp. Correzioni dell'ora, DST e RTC arretrato non riordinano nulla.
2. **Il core non legge orologi.** Il tempo è un'annotazione del runtime, campionata **una volta per operazione applicata** e registrata nel journal; il replay riusa i valori registrati e non ricampiona.
3. **Nessuna correzione invisibile.** Ogni timestamp osservato è conservato *raw*, con provenienza e qualità; le anomalie (regressione, incertezza) sono diagnosticate e registrate, mai «aggiustate».
4. **Un timestamp riportato non è autenticato** per il solo fatto di esistere: il valore di una sorgente è *reported*.

## 2. I sei tempi (brief §4.1)

| # | Nome | Chi lo produce | Dove vive | VS-006 |
|---|---|---|---|---|
| 1 | `source_occurred_at_utc_ns?` | apparato che ha registrato l'evento originario | sidecar per osservazione (wire 1.2), journal | **trasporto e conservazione** se il driver lo invia; `null` altrimenti |
| 2 | `driver_acquired_at_utc_ns?` | driver, all'acquisizione/recupero del frame | sidecar per batch (wire 1.2), journal | come sopra |
| 3 | `runtime_received_at_utc_ns?` | runtime, alla fine della lettura del frame | journal (`journal_operations`) | campionato solo con persistenza attiva |
| 4 | `sequenced_at_utc_ns` | sequencer, subito prima di `apply` dell'operazione **applicata** | journal; condiviso dagli eventi derivati | **sempre** con persistenza attiva |
| 5 | `durable_at_utc_ns?` | writer, dopo il `COMMIT` che copre l'operazione | `journal_commits` (scritto nel commit successivo) | metrica di lag, mai tempo della transizione |
| 6 | `hmi_received_at?` | frontend | **non** nel journal | fuori dall'autorità del journal |

Un input rifiutato non ha `sequenced_at` (il campione viene scartato); nessun `sequenced_at` corrisponde a un evento non confermato. `runtime_received_at` ≠ `sequenced_at` sotto congestione: la differenza è il tempo in coda.

## 3. Envelope di un'operazione (`TimeEnvelope`)

Campionato dal `TimeAuthority` sul thread del sequencer, nel record journal (fuori dal record caldo `AlarmEvent` di 48 B, che resta invariato):

| Campo | Tipo | Semantica |
|---|---|---|
| `sequenced_at_utc_ns` | `i64` | `CLOCK_REALTIME` raw, nanosecondi dall'epoca Unix |
| `boottime_ns` | `u64` | `CLOCK_BOOTTIME` (conta anche la sospensione) al campione |
| `boot_ref` | `u32` | riferimento a `runtime_boots` (che porta `os_boot_id` di `/proc/sys/kernel/random/boot_id`) |
| `logical_time_ns` | `i64` | chiave temporale **non regressiva**: `max(logical_precedente, sequenced_at)`; tie-breaker = `operation_seq` |
| `clock_quality` | `u8` | `Unknown`, `Synchronized`, `Holdover`, `Fault` (diagnostica dell'host, §5) |
| `clock_flags` | `u8` | `REGRESSED` (realtime < realtime dell'operazione precedente), `LOGICAL_HELD` (logical ≠ realtime), `CROSS_BOOT_REGRESSION` (primo campione del boot < ultimo persistito) |

`logical_time_ns` **non è etichettato come UTC fisico certo**: durante un'ora sbagliata è solo una chiave monotona. Il suo ultimo valore è persistito (`schema_meta.last_logical_time_ns`) e riparte da lì dopo il riavvio, quindi non regredisce neppure con RTC arretrato.

Monotonic e boottime **non si confrontano mai fra boot diversi**: ogni valore porta il `boot_ref`. Non si sottraggono due monotonic di boot diversi.

## 4. Orologi del sistema operativo

- **Timeout, watchdog, durate:** `std::time::Instant` (`CLOCK_MONOTONIC`) come in VS-004, mai `CLOCK_REALTIME`. Scelta documentata: i timer di supervisione **non** sono suspend-aware (una sospensione dell'host non deve far scadere in blocco tutte le sorgenti al risveglio senza una nuova osservazione; al risveglio il primo tick valuta il tempo trascorso secondo `CLOCK_MONOTONIC`, che sulla piattaforma Linux non avanza durante la sospensione). Il `boottime_ns` dell'envelope invece è suspend-aware per la diagnostica.
- **Ora legale:** solo **conversione di visualizzazione**. Si persiste UTC; il fuso IANA (default `Europe/Rome`) si applica in lettura, con offset esplicito e segnalazione di ambiguità (le due `02:30` del ritorno all'ora solare hanno offset `+02:00` e `+01:00`) e di inesistenza (le `02:30` del passaggio all'ora legale). Nessuno *sleep* o ricalcolo al cambio d'ora. Le schedulazioni di calendario (politica fold/gap) sono di una slice futura.
- **Clock regression:** realtime di un'operazione < realtime della precedente di più di `clock_regression_alert_ms` ⇒ flag `REGRESSED`, riga in `time_observations` (`ClockRegression`, delta), contatore, messaggio throttled su stderr. Nessun riordino, nessun evento retrodatato.
- **Salti in avanti** (es. +24 h): registrati come anomalia (`ClockJumpForward`) se superano `clock_jump_alert_ms`; la chiave logica li segue (non può «ritardare» l'ora vera senza sapere quale sia giusta).

## 5. Qualità del clock dell'host

Il sequencer non governa l'orologio del SO. Un thread diagnostico (periodo `[time] host_sync_probe_period_ms`, default 60 s) interroga, se configurato (`host_sync_probe = "none" | "chronyc" | "timedatectl"`, default `"none"`), lo stato di sincronizzazione dell'host (chrony/NTP: stratum, offset stimato, leap status; systemd-timesyncd: `NTPSynchronized`) e pubblica qualità e incertezza in un atomico letto dal sequencer (costo per operazione: un `load`). Default `"none"` ⇒ qualità `Unknown`: VS-006 **non dichiara** l'ora sincronizzata senza prova. PTP: il probe è estendibile, nessun supporto in questa slice.

## 6. Timestamp di sorgente (wire 1.2, sidecar)

- Campi **additivi e opzionali**: `ObservationBatch.source_timestamps_utc_ns = 7` (`repeated sfixed64`, **allineato** a `observations`: l'elemento *i* è il timestamp dell'osservazione *i*; `INT64_MIN` = assente per quell'osservazione; lista vuota = nessun timestamp), `ObservationBatch.acquired_at_utc_ns` (`optional sfixed64`), `ObservationBatch.source_clock_quality` (enum), `ObservationBatch.source_clock_id` (string ≤ 64 B). Rappresentazione `int64` nanosecondi UTC; range valido `[0, 2^63)` (1970–2262). Lunghezza diversa da quella di `observations` o valore negativo diverso da `INT64_MIN` ⇒ batch rifiutato (`ERROR_CODE_INVALID_TIMESTAMP` = 48) senza effetto.
- **Scostamento dalla prima stesura (deciso in implementazione):** la stesura iniziale prevedeva `Observation.source_timestamp_utc_ns = 4` per osservazione. Aggiungere un campo a `Observation` porta la struttura decodificata `pb::Observation` da 32 a 40 B e sfora il bound di memoria VS-005 del profilo server (16 MiB per le strutture di decodifica preallocate). Il campo parallelo nel batch lascia invariata `Observation` (bound VS-005 identico) e costa zero byte ai driver che non lo inviano. Il campo 4 di `Observation` non è mai stato rilasciato (nessuna incompatibilità wire); il numero resta libero.
- Il kernel conserva i timestamp di sorgente **solo** con `[persistence]` attivo e link negoziato ≥ 1.2; senza journal non c'è dove conservarli e il vettore è scartato subito dopo la validazione (nessuna memoria trattenuta).
- I timestamp sono **sidecar runtime/journal**: non entrano nel `Value` del core, non cambiano digest, deduplicazione né confronto dei batch (due batch identici nei valori e diversi nei timestamp restano `DUPLICATE`; il secondo set di timestamp **non** sostituisce il primo).
- Conservati **invariati** nel payload dell'operazione; per ogni evento d'allarme derivato da una regola su un tag osservato in quel batch, `origin_ts` = timestamp di sorgente di quell'osservazione (se presente), con provenienza `source`; altrimenti `null`. `null` significa «non disponibile», mai «uguale a `sequenced_at`».
- Un driver 1.0/1.1 non li invia: tutti `null`. Un kernel che non conosce 1.2 li ignorerebbe (campi sconosciuti protobuf), quindi un driver 1.2 deve inviarli solo dopo aver negoziato 1.2.
- Il valore è *reported*: il kernel non verifica che provenga davvero dall'apparato originario (principio OPC UA `SourceTimestamp`: chi lo inoltra non lo modifica). La documentazione del driver deve dichiararne l'origine.
- Un dato tardivo con `source_timestamp` più vecchio è lecito: la sequenza SCADA resta quella di acquisizione.

**Gate di fattibilità (brief §4.2).** Il trasporto wire 1.2 è additivo e non cambia il comportamento dei driver legacy, quindi è incluso. L'ingest di **eventi nativi** bufferizzati (con propri timestamp) è differito (ADR 0013 §D10).

## 7. Replay deterministico

- Il replay legge `TimeEnvelope` dal journal; non chiama mai `TimeAuthority`.
- `durable_at` non entra in alcun digest.
- Test: digest di stato e digest degli eventi identici con orologi iniettati diversi (DST, offset UTC, regressioni); timestamp raw identici dopo il recupero.

## 8. Diagnostica (`GetTimeStatus`)

`observed_utc` dell'ultima operazione, `logical_time`, `boot_ref` e `os_boot_id`, qualità e incertezza dell'host, contatori `clock_regressions` / `jumps_forward` e ultima anomalia, metadati NTP se disponibili. Nessuna misura costosa per query: si leggono valori già pubblicati.

## 9. Limiti dichiarati

- Senza probe configurato la qualità è `Unknown` (nessuna affermazione di sincronizzazione).
- Il `TimeAuthority` vede solo l'orologio dell'host: un host con orologio sbagliato e stabile produce `sequenced_at` sbagliati ma monotoni; solo il confronto con una fonte esterna lo rivela.
- I timestamp di sorgente sono quanto dichiarato dal driver.
- Le prove di salto d'orologio usano un orologio iniettato (`--test-clock-offset-file`), non un vero step del clock del SO (che richiederebbe privilegi sull'host di test): **NOT TESTED WITH A REAL OS CLOCK STEP**.
