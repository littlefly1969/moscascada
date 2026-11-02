# VS-007 — Driver SDK modulare e primo driver Modbus TCP (MoScada)

**Stato:** SPECIFICA ESECUTIVA / PR Draft iniziale. Questo documento non afferma che il codice o i benchmark siano stati già sviluppati.

**Base:** PR #6 (VS-006, commit `3a409caca6cf8ca8793233c2bb2e931e1f3cfc21`). La PR #6 non è ancora mergiata. **Non mergiare automaticamente VS-006, EXP-001 #7 né VS-007.** Dopo autorizzazione al merge di VS-006, aggiornare la base di VS-007 su `main` e rilanciare la CI. **Decisione storage:** issue #8; mantenere SQLite, nessun open-wal nel runtime.

## 0. Scopo e criterio di slice

Costruire una prima vertical slice eseguibile completa: **driver Modbus TCP di sola lettura** che, usando uno **SDK comune**, legge un dispositivo simulato o reale, mappa punti del catalogo prodotto verso i canali MoScada e invia osservazioni al kernel esistente tramite `scada.driver.v1` (minor 1.2 quando negoziato). Testare dal TCP Modbus alla RTDB, Alarm Engine, storico SQLite e recovery, senza cambiare il kernel.

Non creare una piattaforma di plugin dinamici nel core. I driver sono **eseguibili esterni** con dipendenze e processi indipendenti. Nuovi protocolli e nuovi prodotti si aggiungono fuori dal kernel, tramite il protocollo driver già versionato e file catalogo validati.

### Deliverable (obbligatori)
1. Crate `scada-driver-sdk`: sessione IPC/handshake, ACK, reconnessione, source epoch, sequenze, FullState/Delta, heartbeat negoziato, receipt durevoli opt-in, bounded pending/diagnostica.
2. Crate `scada-modbus` (codifica/trasporto + scansione, separata dai mapping di prodotto), binario `scada-driver-modbus-tcp`. Isolamento dei guasti per endpoint; nessuna modifica a `Kernel::apply`.
3. Catalogo validato: schema versionato di mapping **trasporto → protocollo → profilo prodotto → canali/tag**, con profilo generico di prova. Nessun marchio o indirizzo di registri inventato come fosse ufficiale.
4. Simulatore di server Modbus TCP e integrazione automatizzata con kernel reale, journal attivo e spento, alarm engine, tempi e riconciliazione.
5. ADR 0015: modello di driver e sicurezza; contratto operativo e playbook; test/benchmark con log e dati grezzi; PR con review indipendente e limiti espliciti.

## 1. Vincoli inderogabili

- **Core e protocollo immutabili:** non modificare `scada-core`, la semantica di `Kernel::apply`, Alarm Engine, Time Authority, SQLite/journal, ingress sequencer, schema Protobuf `scada.driver.v1` minor 1.2; eventuale bug upstream → issue/PR separata. SDK e driver sono consumer del contratto esistente.
- `source_id` e mapping `channel_id` appartengono a configurazione MoScada; `driver_id`, `source_epoch` e `source_sequence` rispettano v1.2. Nessuna scansione dipende da ordini non deterministici di HashMap.
- `FullState` solo quando **tutti** i canali configurati per una sorgente hanno ricevuto una lettura valida nel ciclo di acquisizione; non inventare campioni `Good` per letture fallite. In generale il polling non è una fotografia atomica del dispositivo: indicare in diagnostica la durata dello scan e il rischio di inconsistenza fra richieste.
- Dopo crash/nuova istanza, inviare prima il `FullState` richiesto dal `Welcome`. Reconnect sulla stessa istanza/epoca: gestire ACK persi, last_accepted e ritrasmissione ordinata dei batch pending. Nessuna riconciliazione `Complete` senza prova di buffer di eventi del campo.
- Distinguere receipt `APPLIED_VOLATILE` da `APPLIED_DURABLE`; negoziare la modalità, non usare il primo come garanzia di persistenza. `APPLIED_NOT_DURABLE` e `JOURNAL_FAULT` non permettono di eliminare silenziosamente i pending.
- Source timestamp **non esiste di default in Modbus TCP**: non spacciare l'ora del polling per timestamp del PLC. Inviare `acquired_at_utc_ns` solo come tempo di acquisizione del driver (misurato dopo lettura / scan e annotato come tale); lasciare `source_timestamps_utc_ns` vuoti, a meno che un profilo futuro decodifichi timestamp nativi verificabili. Qualità clock conservativa.
- Boundedness: max endpoints, channels, pending, frame sizes, timeouts, retry, data retention e buffer di lettura; nessuna coda infinita, nessun spin/retry aggressivo, mai drop silenzioso.
- **Soltanto letture** Modbus VS-007; FC 05/06/15/16 e comandi di campo fuori scope finché non esiste un comando autorizzato e idempotente. Nessuna prova di scrittura a dispositivi reali.

## 2. Struttura driver riutilizzabile

Moduli proposti (nomi indicativi, evitare duplicazione di codice):

```
apps/scada-driver-modbus-tcp/  # processo con CLI/config e health
crates/scada-driver-sdk/       # riusa scada-protocol, non dipende da scada-runtime
crates/scada-modbus/           # codec/cliente Modbus TCP, scanner, decoding
docs/adr/0015-driver-sdk-and-modbus-tcp.md
docs/driver-catalog/           # schema + generic.example.toml + README
config/examples/modbus-tcp.toml
docs/benchmarks/vs007.md
```

Lo SDK espone un client con stato esplicito:
`Disconnected → Connecting → Handshaking → AwaitingFullState → Streaming → Backpressured/Recovering`.
Ogni transizione registra motivo e contatori. Scegliere se estrarre helper da `apps/scada-sim-driver/src/lib.rs` o adattarne piccoli metodi: **non far dipendere il driver di produzione dal simulatore** e non introdurre dipendenze cicliche. Il simulatore resta golden client di test.

Il driver Modbus separa protocollo/trasporto dalla trasformazione per prodotto. Un profilo prodotto definisce blocchi di lettura, offset e tipi, scala/politiche e mapping di canali: aggiungere un prodotto NON richiede ricompilare il core o modificare il journal. Una versione futura potrà aggiungere SPABUS, RPIfibus/Profibus e dispositivi intrusion/fire con eventi nativi (non fare finta che Modbus polling ne provi la continuità).

## 3. Modbus TCP: contratto wire e decoder

Riferimenti ufficiali: Modbus Organization, Application Protocol V1.1b3 e TCP/IP Messaging Implementation Guide V1.0b:
- https://www.modbus.org/modbus-specifications
- https://modbus.org/docs/Modbus_Application_Protocol_V1_1b3.pdf
- https://modbus.org/docs/Modbus_Messaging_Implementation_Guide_V1_0b.pdf

Prima versione **FC 01 Read Coils, 02 Read Discrete Inputs, 03 Read Holding Registers, 04 Read Input Registers**, senza writes. Modbus TCP su stream TCP: MBAP header (transaction ID, protocol ID=0, length che conta Unit ID+PDU, Unit ID), nessun CRC RTU; parsing framed anche con segmentazione/coalescenza TCP, limite ADU conservativo secondo la specifica. Validare transaction ID, protocol ID, unit ID, lunghezze, funzione, byte count, risposta di eccezione e quantità; mai accettare risposta vecchia/di altro dispositivo come corrente.

- Quantità massime: 2000 bit per FC01/02, 125 registri per FC03/04; intervalli e overflow indirizzi verificati prima dell'I/O. Indirizzo PDU **zero-based**, interfaccia/catalogo chiarisce esplicitamente che le etichette 4xxxx/3xxxx non si sommano automaticamente all'indirizzo.
- Risposte con exception code, timeout, chiusura, valori non coerenti e pacchetto parziale sono errori distinti e osservabili. Timeout con lettura parziale → non riutilizzare byte del frame vecchio dopo reconnessione.
- Prediligere inizialmente **una richiesta Modbus per connessione alla volta**; niente pipeline senza test di correlazione transaction ID e controllo delle risposte fuori ordine. Config `unit_id` per supportare gateway TCP/RTU.
- Raggruppare letture contigue **per area e policy** senza oltrepassare intervalli illeggibili, né generare letture di registri non documentati in modo indiscriminato. Limiti di rate configurabili.
- Tipi: `bool`, `u16/i16`, `u32/i32` su 2 registri, bit mask, eventuale float32 SOLO se conversione a i64 fixed-point è deterministica e validata (scale/rounding/overflow, endian word/byte esplicito, NaN/Inf rifiutati); i soli valori accettati da RTDB oggi sono `bool`/`i64`. Mai cambiare la RTDB in questa slice.
- Evitare `unwrap` su frame/input non fidati, allocazioni non bounded e blocco indefinito dei task; privilegiare una crate Rust Modbus mantenuta se audit/advisories/licenze la rendono idonea, altrimenti codec piccolo e testato; registrare scelta e versione precisa in ADR.

## 4. Semantica di acquisizione e guasti

Per una sorgente Modbus un ciclo consiste in letture finite e validate, normalizzazione deterministica, confronto con ultima baseline **confermata da MoScada** e pubblicazione di una `FullState` completa o `Delta` contenente solo valori effettivamente cambiati. Primo scan dopo connessione o recovery → `FullState`; dopo singolo errore di lettura **non** inviare FullState parziale e non mantenere indefinitamente `Good` fittizio. Soluzione minima: chiudere la sessione IPC verso kernel per forzare `SourceDisconnected`, rendere i valori `Uncertain(SourceUnavailable)`, ritentare il dispositivo con backoff; al recupero nuovo FullState. Documentare l'impatto sullo storico degli allarmi e sui latch.

`source_epoch`: univoca per avvio di driver, nonzero; se il processo muore senza spool durevole non affermare di aver conservato eventi/acquisizioni pendenti. Dentro lo stesso processo, reconnect conserva epoch e batch non confermati nel limite `max_inflight`, riusa la stessa source_sequence dei retry. In caso di perdita del processo, nuovo epoch → riconciliazione live, continuità `Unknown`. Un driver polling non può produrre `Complete`.

Non confondere Modbus `transaction_id` (richiesta/risposta TCP) con `source_sequence` (ordine ingress al kernel), `operation_sequence` o `event_seq`.

## 5. Configurazione e sicurezza

Usare TOML con schema/versione validata e modalità `--check-config` **senza avviare socket né collegarsi ai dispositivi**. Per ogni device: endpoint IPv4/IPv6/hostname o configurazione equivalente, porta default 502, `unit_id`, intervalli di scan, timeout connect/read, max retry/backoff, source_id, canali e tipo, profilo prodotto/versione. Rifiutare mapping canali duplicati, overlap ambigui, tipo non coerente, scale impossibili, indirizzi fuori bounds, scansioni che non possono diventare `FullState`, mapping non presente nella config kernel, almeno dove disponibile con controllo statico. Non affidarsi a una semplice validazione di sintassi TOML.

Modbus TCP standard **non autentica né cifra** le connessioni: usare rete OT segmentata, ACL/firewall, unità allowlisted, credenziali/permessi minimi per processi MoScada, UDS locale autorizzato. Documentare i limiti, e rinviare Modbus Security/TLS ad una slice dedicata. Non fare scansioni/discovery aggressive e non esporre server di controllo aperto alla rete.

## 6. Test di accettazione obbligatori

| ID | Prova | Criterio |
|---|---|---|
| T01 | Config/catalog invalidi, limiti, duplicati e conversioni | fail-closed, zero connessioni su `--check-config` |
| T02 | FC01/02/03/04 e MBAP | dati esatti; zero-based; exceptions e byte-count controllati |
| T03 | Fragmentation, coalescing, wrong transaction ID/unit, timeout, EOF | nessuna lettura fantasma, niente riutilizzo di frame |
| T04 | Un device con 3+ tipi e più blocchi | `FullState` completo con canali esatti |
| T05 | Delta after FullState, no-change scan | sequenza monotona, nessun input superfluo non richiesto |
| T06 | Crash/disconnect del server Modbus a metà scan | RTDB degradata, nessun FullState parziale/false Good |
| T07 | Riavvio del kernel con journal VS-006 | recuperato=Unverified, FullState prima di Live=Verified |
| T08 | Loss ACK/durable receipts stalled | nessun ACK durevole prematuro, pending bounded, retry stesso source_sequence |
| T09 | Reconnect kernel/driver, epoch nuovo dopo restart del processo | gestione corretta duplicati/gap e storico `Unknown` |
| T10 | Allarme su punto Modbus + disconnessione/recovery | niente falso rientro; id/occurrence/ACK persistenti |
| T11 | Polling e timestamp | solo `acquired_at` driver; nessun timestamp nativo inventato; clock quality corretta |
| T12 | Fuzz MBAP/PDU, parsing catalogo e decoder valore | no panic, no OOM, reject bounded |
| T13 | Backpressure con SQLite lento, FULL/outbox saturi | rifiuti tipizzati, nessuna silent data loss nel driver finché il suo buffer è integro |
| T14 | Compatibilità protocollo e default-off | `buf breaking`, VS-001..006 test e digest invariati |
| T15 | 30 min soak con server Modbus locale + journal, disconnessioni pilotate | RSS bounded, nessun deadlock, risultati grezzi |
| T16 | x86_64 e aarch64 | almeno check completo; test su hardware Micro se disponibile, altrimenti limite dichiarato |

Simulatore Modbus deve permettere iniezioni controllate (exception, frame troncato, bad header, ritardo, reconnect, mutazione valori durante scan), con seed riproducibile. Aggiungere fixture con almeno un profilo generico e un esempio nel catalogo. Eseguire `./scripts/check.sh`, test del nuovo crate ed E2E; CI e `cargo deny` senza warning. Test fuzz e crash non devono trasformare failure reali in skip silenziosi.

## 7. Performance e budget

Misurare acquisizioni complete/s, registri/s, osservazioni/s, op/s applicate e durevoli, p50/p99/p99.9 scan→kernel→journal, CPU/RSS per processo, numero di connessioni e retry, capacità massima di canali configurabili, perdita allarme/dati per errore di dispositivo e spool budget. Confrontare journal SQLite on/off, FIFO/source_fair, current_thread/multi_thread, 1/8/16 device e blocchi 1/16/125 registri. Testare sia scan lento realistico sia throughput locale saturo; la porta TCP e il simulatore devono essere su loopback, non su impianto reale.

La frequenza massima **non** è un valore da inventare; derivare budget dai tempi di risposta e dall'indice di variazioni, pubblicare il capacity model e limiti Micro/Server.

## 8. Gate per review indipendente

- Commit incrementali: (a) ADR e SDK, (b) protocollo Modbus scanner, (c) catalogo e integrazione kernel, (d) test, benchmark e playbook. Separare clearly `done`, `partial` e `not tested`.
- Nessun cambio alla VS-006 baseline, al journal, al kernel o al protocollo pubblico; ogni divergenza va motivata PR separata.
- Nuovo driver è plug-in di **processo**, non caricamento dinamico di libreria nel kernel.
- **Nessun merge automatico**, né questa PR né PR #6/#7. A completamento chiedere una review indipendente di correttezza e performance.
- La scelta dello storico (SQLite Micro / PostgreSQL Enterprise) è futura: **non implementare PostgreSQL in VS-007**. La decisione di mantenere SQLite e rinviare open-wal è registrata nella issue #8.

## 9. Istruzione per l'agente

Implementare le fasi della VS-007 **nel branch di questa PR**, senza chiedere di rifare il kernel e senza sottintendere che questo documento sia codice già eseguito. Partire leggendo il contratto VS-006, gli helper `apps/scada-sim-driver/src/lib.rs` e il file `proto/scada/driver/v1/driver.proto`; usare gli invarianti della presente specifica come gate. Non dichiarare test real-hardware o power-loss senza averli effettivamente eseguiti.
