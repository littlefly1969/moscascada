# ADR 0015 — Driver SDK comune e primo driver Modbus TCP (sola lettura)

- **Stato:** **proposto per review indipendente** (VS-007). Le sezioni «Verifica» rimandano alla matrice T01–T16 di `docs/traceability.md` (VS-007) e ai risultati grezzi di `docs/benchmarks/vs007.md`.
- **Data:** 2026-11-03
- **Base:** PR #6 (VS-006, `3a409ca`), non ancora mergiata. Nessun cambiamento di `scada-core`, `scada-runtime`, `scada-journal`, `scada-protocol` né degli schemi Protobuf.
- **Ambito:** nuovi crate `scada-driver-sdk`, `scada-modbus`, `scada-modbus-sim`, nuovo processo `scada-driver-modbus-tcp`, catalogo `docs/driver-catalog/`.
- **Specifica:** `docs/vertical-slices/vs007-driver-sdk-modbus-tcp.md`.

## 1. Contesto

Fino a VS-006 l'unico produttore di osservazioni è `scada-sim-driver`, golden client di test. VS-007 introduce il primo driver di campo. Il vincolo è che **nuovi protocolli e nuovi prodotti si aggiungano fuori dal kernel**: il kernel conosce soltanto il contratto `scada.driver.v1` (fino al minor 1.2) e la propria configurazione `[[sources]]`.

## 2. Decisioni

### D1 — Un driver è un processo, non un plug-in

Un driver è un **eseguibile esterno** con le proprie dipendenze, i propri thread e il proprio ciclo di vita, collegato al kernel solo tramite il socket Unix del driver protocol. Nessun caricamento dinamico di librerie nel kernel, nessuna ABI Rust condivisa, nessun registro di plug-in. Conseguenze: un crash, un deadlock o una dipendenza vulnerabile di un driver non entrano nello spazio di indirizzi del kernel; il kernel non cambia per aggiungere un protocollo; l'autorizzazione resta `authorized_uids` per sorgente (`SO_PEERCRED`, ADR 0009).

### D2 — `scada-driver-sdk`: unico punto in cui vive la semantica di sessione

Grafo delle dipendenze (nessun ciclo, verificato da `cargo tree`):

```
scada-driver-modbus-tcp ──► scada-driver-sdk ──► scada-protocol
          │                                         ▲
          └──────────────► scada-modbus (std only)  │
scada-modbus-sim ──► scada-modbus                    │
scada-sim-driver ────────────────────────────────────┘  (golden client di test, invariato)
```

Lo SDK **non** dipende da `scada-runtime`, `scada-core` o `scada-journal` né da `scada-sim-driver`; il driver di produzione non dipende dal simulatore (solo `dev-dependencies` dei test). Il simulatore driver VS-001..006 resta invariato e resta il golden client dei test del kernel: estrarre i suoi helper avrebbe toccato test e digest esistenti senza beneficio; lo SDK riscrive la parte di sessione in forma non bloccante e con stato esplicito.

`SourcePublisher` = una sessione logica per una `source_id` (contratto §1: una connessione = una sorgente). Stato esplicito e registrato con motivo:

| Stato | Significato | Esce con |
|---|---|---|
| `Disconnected` | nessun link | `ensure_connected` dopo il backoff |
| `Connecting` / `Handshaking` | `connect` + `Hello` → `Welcome` / `HandshakeRejected` | `Welcome` |
| `AwaitingFullState` | link pronto, il prossimo batch è (o è in coda) un `FullState` | ACK del `FullState` |
| `Streaming` | un `FullState` di questo link è stato applicato; i `Delta` fluiscono | rifiuto, guasto, chiusura |
| `Backpressured` | rifiuto ritentabile (`OUTBOX_CAPACITY_EXCEEDED`, `JOURNAL_QUEUE_FULL`, `DISK_FULL`, `KERNEL_SHUTTING_DOWN`) o finestra piena | ACK applicato del batch rifiutato |
| `Recovering` | `JOURNAL_FAULT`, `APPLIED_NOT_DURABLE`, rifiuto non ritentabile | nuova istanza del kernel / `FullState` applicato |

Ogni transizione conserva `(istante, da, a, motivo)` in un anello bounded (`transitions_kept`) e i contatori monotoni (`Counters`) sono esposti al processo driver.

### D3 — `FullState` / `Delta`, baseline e sequenze

- `source_epoch`: 64 bit da `/dev/urandom`, ≠ 0, **nuova a ogni avvio del processo** e mai persistita: il driver non ha spool durevole e non può affermare di aver conservato acquisizioni pendenti attraverso il proprio riavvio. Dopo un riavvio del driver il kernel risponde `full_state_required` e la continuità storica è `Unknown` (ADR 0013 §10.4 punto 5).
- `source_sequence` è per epoca, cresce di 1 per batch **costruito**, non torna mai indietro (neppure dopo un riavvio del kernel: `next = max(next, last_accepted + 1)`), è riusata identica nei reinvii. Il `transaction_id` Modbus è un concetto diverso (correlazione richiesta/risposta TCP, D6) e non è mai confuso con `source_sequence`, `operation_sequence` o `event_seq`.
- `publish_scan(values, acquired_at)` accetta solo una scansione **completa** (un valore del tipo corretto per ogni canale configurato, ordinato per `channel_id`; altrimenti `IncompleteScan` prima di qualsiasi I/O). Il primo scan dopo ogni (ri)connessione è un `FullState`; poi `Delta` con i soli canali cambiati rispetto allo stato **proiettato** (= ultimo `FullState` + `Delta` costruiti e non annullati). Nessun cambiamento ⇒ nessun batch (`NoChange`).
- Un `FullState` è inviato **solo con nulla in volo**: non può superare un batch rifiutato e applicarsi come `RESYNCHRONIZED` mentre il batch precedente verrebbe poi reinviato come `STALE`.
- Nessuna `Continuity::Complete` (non esiste nel wire) e nessuna affermazione di continuità: lo SDK riporta solo quella dichiarata dal kernel (`BASELINE_ESTABLISHED`, `CONTINUOUS`, `RESYNCHRONIZED`).

### D4 — Finestra pending bounded, ACK e receipt

Ogni batch costruito entra in una coda **bounded** (`max_pending`, ≤ 65 536) con stato `Queued` → `InFlight` → (rimosso) oppure `AppliedAwaitingDurable`. In volo al più `min(max_inflight, Welcome.limits.max_inflight_batches)`. Gli ACK devono arrivare nell'ordine di invio e portare il `kernel_instance_id` del link; altrimenti il link è chiuso (desincronizzato) e riconciliato al reconnect.

| Risposta | Effetto sul pending |
|---|---|
| `APPLIED_VOLATILE` (link senza receipt durevoli) | rimosso: applicato in RAM, **nessuna promessa di persistenza** |
| `APPLIED_DURABLE` / `DUPLICATE` su link durevole | rimosso con tutto il prefisso; `durable_confirmed_sequence` avanza (il `DUPLICATE` durevole attende il commit di tutte le operazioni applicate, review PR #6) |
| `APPLIED_NOT_DURABLE` | **conservato** (`AppliedAwaitingDurable`, `not_durable`), stato `Recovering`, nessun nuovo batch; risolto solo da una nuova istanza del kernel (D5) |
| `REJECTED` ritentabile | stesso batch, **stessa `source_sequence`**, reinviato dopo backoff esponenziale bounded; i batch già in volo dopo di esso ricevono rifiuti collaterali (`SEQUENCE_GAP`/`RESYNC_REQUIRED`) attesi e vengono reinviati nell'ordine |
| `REJECTED` non ritentabile | batch annullato, annullati anche i successivi costruiti sopra (contati), `FullState` al prossimo scan dopo backoff |

Finestra piena ⇒ lo scan **non è pubblicato** (`Deferred`, contato) e lo stato è `Backpressured`; lo scan successivo porta tutte le differenze rispetto allo stato proiettato, quindi nessun **cambiamento** di valore è perso, solo i campioni intermedi (che un driver a polling non può garantire comunque). Mai drop silenzioso di un batch costruito: ogni batch esce dalla coda per ACK, per annullamento contato o per perdita dichiarata dopo un riavvio del kernel (D5).

Receipt durevoli: `request_durable_receipts` (Hello 1.2) è una richiesta; il link è durevole solo se `Welcome.durable_receipts` e minor ≥ 2. `require_durable_receipts = true` rifiuta di trasmettere su un link non durevole. `APPLIED_VOLATILE` non è mai trattato come garanzia di persistenza.

### D5 — Riconnessione e riavvii (contratto §5, §5.1, §7, §10.4)

Al `Welcome`, con `L = last_accepted_source_sequence`:

- **Stesso processo, stessa epoca, kernel con baseline** (`full_state_required = false`): i batch `≤ L` sono applicati (ACK perso). Link volatile: rimossi. Link durevole: il batch `L` è reinviato per ottenere un `DUPLICATE` che vale prova durevole del prefisso; quelli `< L` attendono quella prova senza essere reinviati. I batch `> L` sono reinviati nell'ordine originale. Poi il primo scan è un `FullState` (continuità `CONTINUOUS`: tutti i canali tornano `Good`, procedura §5.1).
- **`full_state_required = true`** (kernel riavviato, o nessuna baseline per l'epoca): i batch `≤ L` sono il prefisso recuperato (durevole se il kernel journala); quelli `> L` sono **dichiarati persi** (`batches_lost_kernel_restart`, intervallo di sequenze nel `last_error`) e sostituiti da un `FullState` fresco con sequenza `> max(L, ultima usata)`. Il kernel registra il gap; i campioni intermedi non sono ricostruiti.
- **Riavvio del driver:** nuova epoca (D3), `BASELINE_ESTABLISHED`, storico `Unknown`.

### D6 — Modbus TCP: codec proprio, piccolo e testato

Scelta: **codec interno `scada-modbus`** (≈ 900 righe, solo `std`, nessuna dipendenza), non una crate esterna. Valutate (crates.io, 2026-11-03): `tokio-modbus 0.17.0` (MIT/Apache-2.0, ottobre 2025: async, porta `tokio`, `async-trait`, `bytes`, `crc`, `thiserror` e implementa anche le scritture), `rmodbus 0.13.0` (Apache-2.0, orientata a server/framework), `modbus 1.1.3` (sincrona, `byteorder` + `enum_primitive`). Motivi: (a) servono solo 4 funzioni di lettura e il controllo esplicito di ogni campo della risposta; (b) nessuna funzione di scrittura deve esistere nel binario (VS-007 §1); (c) nessuna nuova dipendenza da auditare in `cargo deny`; (d) la politica sui frame non validi (connessione avvelenata, D7) deve essere quella della specifica, non quella di una libreria. Una crate esterna potrà sostituire il codec dietro la stessa interfaccia se un audit futuro lo giustifica.

Riferimenti: Modbus Application Protocol V1.1b3 (§6.1–6.4, §7) e Modbus Messaging on TCP/IP Implementation Guide V1.0b (§3.1.3).

- **MBAP:** `transaction_id` u16, `protocol_id` = 0, `length` = unit id + PDU (2..=254), `unit_id`; nessun CRC. Decoder incrementale con buffer fisso di 260 byte (ADU massimo): segmentazione e coalescenza TCP trasparenti, header invalido rifiutato **prima** di leggere il PDU.
- **PDU:** FC01/02 (1..=2000 bit), FC03/04 (1..=125 registri); `address + quantity ≤ 65 536` verificato **prima dell'I/O**. Indirizzi **PDU zero-based**: le etichette `0xxxx/1xxxx/3xxxx/4xxxx` della documentazione dei prodotti non sono mai sommate o sottratte; il catalogo usa solo indirizzi PDU.
- **Risposta:** funzione = richiesta oppure eccezione (`fc | 0x80` + codice, PDU di esattamente 2 byte); byte count = quello implicato dalla quantità; lunghezza PDU coerente; bit di padding a zero (FC01/02); `transaction_id` e `unit_id` uguali alla richiesta.
- **Una richiesta per connessione alla volta**, nessuna pipeline (nessuna correlazione fuori ordine da verificare).

### D7 — Errori distinti e connessione avvelenata

`ModbusError` distingue `Timeout{partial_bytes}`, `Closed{partial_bytes}`, `Io`, `Frame` (header MBAP), `TransactionMismatch`, `UnitMismatch`, `Response(Exception | FunctionMismatch | ByteCount | Length | NonZeroPadding)`. **Tutti tranne l'eccezione avvelenano la connessione**: il client rifiuta ulteriori richieste (`Poisoned`) e il driver la chiude e ne apre una nuova; i byte parziali sono scartati (`AduDecoder::reset`) e non sono mai combinati con una risposta successiva. Una risposta vecchia (es. duplicato coalescente) arriva con il `transaction_id` precedente ed è rifiutata come `TransactionMismatch`: mai accettata come corrente. Contatori per tipo di errore nel client.

### D8 — Pianificazione delle letture e decodifica dei valori

- Il profilo dichiara i **blocchi leggibili** (area, inizio, quantità, `max_per_request` ≤ limite del protocollo). Il planner non legge fuori da un blocco, non unisce blocchi diversi (un buco fra blocchi può essere un intervallo che risponde ILLEGAL DATA ADDRESS), non legge code inutilizzate, non spezza mai un valore a 32 bit fra due richieste, rifiuta blocchi sovrapposti e piani oltre `max_requests_per_scan`. Ordine delle richieste `(area, indirizzo)`: indipendente dall'ordine di input e da qualsiasi HashMap.
- Tipi: `bit` (coil/discrete input), `register_bit` (bit 0..=15 di un registro), `u16`, `i16`, `u32`, `i32` (due registri, `word_order` `high_first`/`low_first` esplicito, `byte_swap` esplicito), `f32` **solo** con scala fixed-point e arrotondamento espliciti (`half_even` o `truncate`). I soli tipi RTDB sono `bool` e `i64`: la RTDB non cambia.
- Scala `round(raw × num / den) + offset` in aritmetica intera a 128 bit (`exact` rifiuta resti, `half_even`, `truncate`), overflow **verificato staticamente** sugli estremi del tipo raw al caricamento del catalogo. `f32`: NaN/Inf rifiutati, conversione in binary64 esatta, moltiplicazione e divisione IEEE senza FMA, risultato entro 2^53 (rappresentabile esattamente), poi arrotondamento: stesso risultato su x86_64 e aarch64. Intervallo di plausibilità opzionale (`valid_min/valid_max`): un valore fuori intervallo è un errore di lettura, non un dato `Good`.

### D9 — Semantica di acquisizione e guasti del dispositivo

Uno scan = tutte le richieste del piano, in ordine, poi decodifica di tutti i canali. **Qualsiasi** errore (trasporto, eccezione, valore non decodificabile) rende lo scan fallito per intero: nessun `FullState` parziale, nessun `Delta` dei canali letti, nessun `Good` fittizio. Soluzione minima della specifica:

1. il driver attende per al più `drain_timeout_ms` gli ACK in volo, poi **chiude la sessione IPC** della sorgente; il kernel applica `SourceDisconnected` e i tag con valore diventano `Uncertain(SourceUnavailable)` con il valore conservato;
2. la connessione Modbus è chiusa se l'errore l'ha avvelenata (D7), conservata dopo un'eccezione o un errore di decodifica;
3. il dispositivo è ritentato con backoff esponenziale bounded (`device_backoff_min_ms..max_ms`);
4. al primo scan completo successivo il driver si ricollega (stessa epoca: replay dei pending, D5) e invia un `FullState` (continuità `CONTINUOUS`).

Impatto su allarmi e storico (ADR 0012 D3): con qualità non `Good` le regole di processo diventano **Indeterminate**: `input_valid = false`, latch invariato, **nessun ritorno automatico a `Normal`** e nessun evento; una regola `bad_quality` sulla sorgente diventa attiva. Al `FullState` di recupero le regole si rivalutano sui valori nuovi: un allarme ancora vero resta lo stesso episodio (stesso `occurrence_id`), uno rientrato produce `Returned` con il tempo di sequenziamento del recupero (il momento reale nel campo è ignoto). Una variazione avvenuta e rientrata durante il guasto è **persa** e non è ricostruita: Modbus a polling non prova la continuità.

Il polling **non è una fotografia atomica** del dispositivo: richieste diverse leggono istanti diversi. Il driver misura e pubblica, per scan, la durata `first request → last response` (`scan_spread`) come finestra di possibile inconsistenza fra richieste. Il catalogo dovrebbe mettere in uno stesso blocco i valori che devono essere coerenti fra loro.

### D10 — Tempo

Modbus TCP non ha un timestamp di sorgente. Il driver invia **solo** `acquired_at_utc_ns` = `CLOCK_REALTIME` campionato **dopo l'ultima risposta dello scan** (tempo di acquisizione del driver, non del PLC), con `source_clock_quality = UNKNOWN` (il driver non verifica la sincronizzazione dell'host) e `source_clock_id` vuoto, e **lascia vuoto `source_timestamps_utc_ns`**. Su un link negoziato < 1.2 nessun campo temporale è inviato. Un profilo futuro potrà decodificare timestamp nativi verificabili (es. registri di un data logger) in una slice dedicata. Durate, timeout e backoff usano `Instant` (`CLOCK_MONOTONIC`).

### D11 — Catalogo e configurazione

Quattro livelli separati:

1. **trasporto** (`tcp`: host, porta, timeout) — configurazione del dispositivo;
2. **protocollo** (`modbus`: unit id, funzioni, limiti) — `scada-modbus`;
3. **profilo prodotto** versionato (`id`, `version`, blocchi, punti, tipi, scale) — file TOML in un catalogo, schema `moscada.driver-catalog/1`, nessuna ricompilazione per un prodotto nuovo;
4. **canali/tag** MoScada (`source_id`, `channel_id` ⇒ tag) — configurazione del driver per dispositivo e configurazione del kernel.

Il catalogo di questa slice contiene **solo** il profilo generico di prova `generic-io` v1: nessun marchio, modello o mappa di registri presentata come ufficiale. `--check-config` valida configurazione e catalogo **senza aprire socket** (né verso il kernel né verso i dispositivi) e, con `--kernel-config`, verifica staticamente contro il TOML del kernel: sorgente esistente, ogni canale configurato presente con lo stesso tipo, **tutti** i canali della sorgente mappati (altrimenti un `FullState` sarebbe impossibile), numero di canali ≤ `max_observations_per_batch`, dispositivi ≤ `max_driver_connections`. Rifiuti: schema/versione sconosciuti, chiavi sconosciute, profilo mancante o versione diversa, punti/canali duplicati, punti fuori dai blocchi, blocchi sovrapposti, scale impossibili, tipi incoerenti con l'area, limiti fuori intervallo, `source_id` duplicate.

### D12 — Sicurezza

Modbus TCP standard **non autentica e non cifra**. Requisiti operativi (playbook): rete OT segmentata, ACL/firewall che consentono al solo host del driver di raggiungere i dispositivi sulla porta 502, `unit_id` configurati per dispositivo, nessuna discovery o scansione di indirizzi (il driver legge solo i blocchi dichiarati, con `inter_request_delay_ms` configurabile e un solo scan in corso per dispositivo), processo con utente dedicato senza privilegi, socket del kernel limitato da `authorized_uids`. Il driver **non apre alcun socket in ascolto**: lo stato è esposto su stdout e su un file di health locale. Il binario non contiene codice di scrittura Modbus (FC05/06/15/16 assenti). Modbus/TCP Security (TLS, porta 802) è rinviato a una slice dedicata.

### D13 — Bound

| Risorsa | Bound |
|---|---|
| dispositivi per processo | `max_devices` (≤ 256) e ≤ `max_driver_connections` del kernel (verifica statica) |
| canali per dispositivo | ≤ `max_observations_per_batch` del kernel |
| richieste per scan | `max_requests_per_scan` del profilo (≤ 256) |
| buffer di lettura Modbus | 260 B per connessione (ADU massimo), nessuna allocazione da campi non fidati |
| frame IPC letti | ≤ 1 MiB, rifiuto dell'header prima dell'allocazione |
| batch pending per sorgente | `max_pending_batches` (≤ 65 536), in volo ≤ `max_inflight_batches` e il limite del kernel |
| timeout | connect / risposta / handshake / scrittura configurati, nessun blocco indefinito |
| retry | backoff esponenziale bounded (dispositivo, kernel, rifiuti ritentabili); nessun spin |
| storico transizioni | `transitions_kept` per sorgente |

Thread: uno per dispositivo (isolamento dei guasti per endpoint: un dispositivo lento o bloccato non ritarda gli altri) più il thread principale di stato.

## 3. Alternative scartate

- **Plug-in dinamici nel kernel** (`dlopen`, WASM): stessa sicurezza del processo più debole, ABI da versionare, contrari al principio del kernel immutabile.
- **Driver async (tokio) con task per dispositivo**: possibile in futuro per centinaia di dispositivi; con decine di dispositivi un thread bloccante per endpoint è più semplice da ragionare (timeout espliciti, nessuna cancellazione a metà frame) e isola meglio i guasti.
- **`FullState` parziale con i soli canali letti**: vietato dal contratto (`INCOMPLETE_FULL_STATE`) e dalla specifica.
- **Mantenere `Good` i valori durante un guasto breve**: vietato (falso `Good`).
- **Persistere la `source_epoch`**: senza spool durevole dei dati sarebbe una falsa promessa di continuità.

## 4. Conseguenze e limiti

- Un guasto anche di una sola richiesta degrada **tutta** la sorgente. Profili con aree di affidabilità diverse vanno modellati come sorgenti distinte (un dispositivo nella configurazione per ciascun gruppo).
- Le variazioni più rapide del periodo di scan possono non essere osservate. La freschezza dichiarata dal kernel (VS-004) misura l'ultimo batch applicato, non l'età fisica del valore.
- La continuità storica di una sorgente Modbus è al massimo `CONTINUOUS` fra batch; mai `Complete`.
- Non testato su hardware reale né su gateway TCP/RTU reali; aarch64 solo compilazione (vedi benchmark).

## 5. Verifica

Matrice T01–T16: `docs/traceability.md` (sezione VS-007). Benchmark e capacità: `docs/benchmarks/vs007.md`. Playbook: `docs/operations/vs007-modbus-playbook.md`.
