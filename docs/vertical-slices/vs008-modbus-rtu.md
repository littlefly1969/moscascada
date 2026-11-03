# VS-008 — Modbus RTU (RS-485) come driver modulare di MoScada

**Stato:** SPECIFICA ESECUTIVA / PR DRAFT iniziale, 2026-11-04. La prima modifica implementata nella PR è *solo* il codec RTU puro e i suoi test unitari. Il driver seriale completo, il simulatore e la qualificazione non sono ancora implementati: NON dichiarare VS-008 DONE in questa fase.

**Base verificata:** main @ 3bb686b05c39f939f3b40b2986c2da07262743b5 (VS-007 mergiata). **Ordine successivo deciso:** VS-009 SPA Bus, VS-010 SNMP. Non iniziarli qui.

## 0. Obiettivo, fuori perimetro e invariant

Aggiungere al sistema Modbus TCP di VS-007 un **nuovo trasporto Modbus RTU**, seriale RS-485, in un processo driver isolato che usa lo stesso Driver SDK, lo stesso PDU FC01–04, lo stesso planner, decoder e catalogo prodotto. **Nessuna modifica a scada-core, Kernel::apply, Alarm Engine, journal SQLite, Time Authority, protocollo scada.driver.v1 o scada-runtime**. Non introdurre alcun comando Modbus di scrittura; la VS-008 rimane di sola lettura.

Definizione di done: un bus seriale con almeno due slave distinti, un processo driver dedicato e il kernel reale; letture FC01–04, osservazioni FullState/Delta, degrado qualità al guasto, recovery journal e storico corretti; fault injection seriale, soak continuo, performance e report riproducibile, review indipendente, CI verde. Preferire il kernel già esistente e nessun aumento della sua superficie di attacco.

## 1. Architettura a strati (minime modifiche)

- **Core immutato:** valore Value::Bool / Value::I64, tag/quality/event/ACK/recovery invariati.
- **scada-driver-sdk invariato** salvo difetto dimostrato: sequenze per sorgente, epoch, receipt durevoli, pending bounded, reconnect, FullState. *Ogni slave* corrisponde ad una source_id e un SourcePublisher; l'epoch cambia a ogni restart del processo, MA non al semplice guasto del bus o riavvio del kernel.
- **scada-modbus::pdu/plan/value invariati:** gli stessi FC01–04, indirizzi zero-based, limiti 2000 bit / 125 registri, 32-bit word order e scaling deterministico.
- **scada-modbus::rtu:** modulo puro, nessun I/O; CRC16 (poly A001 init FFFF, CRC low byte first), ADU di max 256 byte, slave ID 1..247, broadcast 0 non permesso per letture, reject 248..255, PDU validato tramite codec esistente, temporizzazioni t1.5/t3.5. Il bootstrap già aggiunge un modulo con questi primi test.
- **Transport seriale POSIX / RS-485:** preferire crate Rust mantenuta come serialport, con versione PINNED (valutare serialport 4.10.1 e licenza MPL-2.0, possibile restrizione del repository UNLICENSED) e default-features=false per evitare libudev se appropriato; audit con cargo-deny. No unsafe nella codebase. Aprire /dev/serial/by-id/.. configurato, non enumerare dispositivi a runtime. Impostare baud, data bits 8, parity even/odd/none, stop 1/2, timeout e raw mode; validare il comportamento RS-485 DE/RE *automatico via adattatore/hardware*, non presumere che il flush software equivalga a transmission complete. Non guidare RTS/DE software senza documentare test reali di turn-around e kernel support.
- **BusArbiter** (un solo owner per porta fisica): proprietà esclusiva della porta, una richiesta Modbus in volo *su tutto il bus*, scheduler round-robin per slave, timeout per domanda, gap prima/dopo frame, rate limiter, max retries per scan, collision/noise recovery; impedire due processi che aprano lo stesso device tramite lock appropriato e segnalazione fail-closed. Divieto di creare un worker seriale *per ogni slave* che parli sulla stessa RS-485.
- **Driver process** scada-driver-modbus-rtu: un worker/owner del bus e publishers distinti per slave; recupero degli errori per slave. Catalogo prodotto versionato condiviso con Modbus TCP. Riutilizzare il validatore/catalogo: estrarlo in un piccolo crate comune o re-export stabile se necessario; niente duplicazione divergente del parser.
- **Simulatore** seriale pseudo-terminal, verificabile in CI Linux con due slave su una PTY e errori iniettabili; non usare un test TCP per dichiarare provato il comportamento seriale.
- **Diagnostica:** stato del bus vs stato di ogni slave, CRC errors, partial/timeout, wrong address, stale data, retries, overruns, scan spread, USB resets, pending/backpressure, durable receipt p50/p99/p99.9, t3.5 gap, source continuity, memory high-water.

## 2. Requisiti seriali da non sottovalutare

Riferimento: MODBUS over Serial Line Specification and Implementation Guide V1.02 (Modbus Organization). RTU senza header MBAP né transaction_id: la correlazione tra richiesta e risposta è ottenuta da **arbitro unico e una richiesta outstanding**, slave address, funzione, formato quantità/byte count e CRC. Una risposta vecchia o tardiva va rifiutata, non associata al nuovo scan. A differenza del TCP, non esiste un transaction_id; una risposta tardiva *identica* alla richiesta successiva può essere indistinguibile, quindi prima di riutilizzare stessa richiesta occorre una fase di quiet recovery dopo timeout, oppure dichiarare il rischio residuale. Non inventare una garanzia impossibile.

- Un frame RTU max 256 byte; delimitazione tramite silenzio ≥ t3.5. Una pausa intra-frame > t1.5 impone scarto del frame. Per baud ≤ 19200 calcolare t1.5/t3.5 da tempo carattere con impostazione reale 8E1, 8O1, 8N2 (11 bit) oppure 8N1 (10 bit). Per baud >19200 raccomandazione 750 µs / 1750 µs. Nessuna attesa attiva (busy loop) nel servizio.
- **Letture da porta USB seriale** restituiscono blocchi di caratteri e perdono la precisione temporale per singolo byte: non sostenere di aver verificato t1.5 per-byte solo misurando tra chiamate read(); richiedere driver/hardware capace di timestamp per byte, oppure framing conservativo per CRC, numero byte attesi, timer e gap di fine frame con limite esplicito.
- CRC16 su (slave address + PDU), low byte first. Verificare CRC e lunghezza prima di usare valori; nessuna allocazione diretta da lunghezze del campo.
- Configurare porta tramite identificatore stabile (/dev/serial/by-id), baud (es. 9600, 19200, 38400), parity/stop, slave IDs unici nel bus. Impedire broadcast di lettura e qualsiasi FC05/06/15/16. Gestire i gateway USB RS-485 e il turnaround: timeout e inter-request delay espliciti, bound seriale legato al byte rate reale.
- **Arbitraggio per bus, non per slave.** In caso di errore di CRC/timeout di uno slave, scartare tutto lo scan di quello slave, marcare la sua source NonGood via disconnessione IPC (meccanismo VS-007), senza forzare gli altri slave Down se il bus resta sano. Se porta scompare fisicamente, degradare tutte le sorgenti sullo stesso bus (evento di link) e ritentare con backoff.
- Schedulare polling che non saturi il bus: per bus definire R_i letture per scansione, frame request/response trasmessi e latenza slave; utilizzo bus ≤ un budget sperimentale conservativo (inizio 50%, misurare). Nessuno slave può monopolizzare; backoff sui guasti.
- Atomicità scan applicativa: se qualunque richiesta di uno slave fallisce, niente FullState/Delta parziale per quello slave; gli altri slave indipendenti proseguono; lo scan fra più richieste non è fisicamente atomico.
- Source time: RTU standard non contiene un timestamp; usare solo acquired_at del driver con qualità di clock Unknown (come VS-007). In un futuro profilo specifico prodotto potrà esserci un device timestamp, ma non inventarlo ora.

## 3. Configurazione e catalogo

Proporre schema versionato, es. moscada.driver-modbus-rtu/1, senza modificare moscada.driver-catalog/1 salvo necessità giustificata:

    [driver]
    driver_id = "modbus-rtu-1"
    kernel_socket = "/run/moscada/driver.sock"
    request_durable_receipts = true
    require_durable_receipts = true

    [[buses]]
    id = "rs485-a"
    device = "/dev/serial/by-id/usb-..."
    baud_rate = 19200
    data_bits = 8
    parity = "even"
    stop_bits = 1
    response_timeout_ms = 300
    inter_request_delay_ms = 5
    max_devices = 32

    [[buses.devices]]
    name = "meter01"
    slave_id = 1
    source_id = 1001
    profile = "generic-io"
    profile_version = 1
    scan_interval_ms = 1000
    # Channel mappings continue to use the existing catalog schema.

Il blocco sopra è **schema proposto**, non una configurazione attualmente caricabile. --check-config deve validare tutto SENZA aprire porta seriale, socket del kernel o file di health. Se --kernel-config fornito, verificare source_id/Channel IDs/tipi/limite batch/connessioni come VS-007. Rifiutare duplicati slave nello stesso bus, stessa porta usata da bus distinti, line timing inconsistente, serial device path relative/insicure, profilazione non validata, rate impossibile per bus. Dispositivi con lo stesso slave_id su **bus diversi** sono ammessi (source_id deve restare unico).

## 4. Test obbligatori

T01 CRC golden vectors, low-order first, test di bit flip, frame length boundary 4/256/257, pdu corretto/errato, 0 e 248..255 rifiutati.
T02 timing 8E1/8N2/8N1 a baud 9600/19200/38400, round-up, gap pre/post-frame, timeout calcolati vs linea reale.
T03 parser e serial read con segmentazione, byte a byte, buffer coalescenti, rumore pre-frame, collisione, garbage, missing CRC, overflow fixed buffer; fail-closed senza false Good.
T04 chiusura/riapertura porta seriale con timeout, EBADF, EIO, unplug/replug USB, blocked read, riassegnazione path by-id.
T05 due/otto slave sulla **stessa** PTY: uno solo outstanding, fairness, no cross-slave misrouting, slave id duplicato in config rifiutato.
T06 late/stale slave reply, CRC errato, wrong slave, risposta eccezione FC03/04, address/length mismatch, inversione/eco; flush e recovery con quiet interval.
T07 una lettura fallita in mezzo allo scan: dati parziali scartati e source non Good; nessun False Normal/Returned su Alarm Engine; altro slave sano prosegue.
T08 full state/delta, reconnect e crash kernel con journal FULL, lost ACK, durable duplicate non prematuro, RecoveryFence invalid e successivo FullState verificato.
T09 zero-allocazioni da frame ostili, bounded coda e memoria, saturazione del bus, pending full, disco pieno e fault del journal; non bloccare il kernel.
T10 scan scheduling con durate variabili, slow slave, inter-request spacing, bus utilization, queue lag e contatori; fairness per round robin.
T11 fuzz CRC/framer/PDU, config/catalog, property tests per sincronizzazione e rifiuto falsi Good.
T12 soak ≥30 min con stream continuo, 2+ slave, kill simulatore, riavvii kernel, disconnessioni PTY e recovery, senza salti silenziosi di sequenze.
T13 aarch64 cargo check completo con serial port support, verificare dipendenze libc/libudev in runner e cross check. Test fisico su USB RS-485 vero per qualificazione Micro.
T14 test con almeno due slave fisici, terminazione/bias, diverse latenze e rumore; annotare test non eseguiti se hardware assente.

## 5. Benchmark / capacità

Misurare (VM x86_64 / PTY vs hardware USB-RS485 REALI distinti):
- per bus e slave: requests/s, scans/s, registri/s, throughput di osservazioni, CRC/errori/s, p50/p99/p99.9 latency scan→ACK volatile/durable, scan spread, schedule jitter, bus occupancy, % rifiuti/backpressure;
- 1, 2, 8, 16, 32 slave; baud 9600/19200/38400/115200; 1/16/125 registri (non tutti i dispositivi reali li supportano); diversi R e delay;
- worst-case response 10/50/200 ms, timeout/prova ritenti, rate di guasti 0/1/10%, disco journal disponibile/saturo;
- CPU, RSS, allocazioni e latency tail sia driver sia kernel, saturazione, T15 30min non falsamente "continua" se il disco ha smesso di accettare scritture;
- modello di capacità: tempo minimo bus ≈ somma dei bit totali / baud + turnaround + response processing + inter-request + t3.5, con margine documentato; non estrapolare loopback TCP.
- default-off vs VS-007 (per core/kernel/proto ecc. diff zero), nel profilo Micro 4 GiB misurare CPU, RAM, fsync reale e stabilità su memoria di massa.

## 6. Gate di sicurezza e merge

1. Nessun acceso a una porta seriale durante parsing --check-config; nessun device auto-discovery o scansione automatica di slave; solo whitelist configurata.
2. Permission Linux utente dedicato, gruppi minimi e ACL seriale; OT network segmentata per gateway seriali, log e health senza segreti; no FC di scrittura.
3. Unsafe proibito. Dipendenze e licenze verificate; evitare dipendenze seriali che richiedano a sorpresa libudev in Micro senza motivazione.
4. PTY test non equivale a qualificazione fisica RS485. Test non eseguiti devono essere marcati NOT TESTED; non dichiarare compatibilità garantita con centrali/prodotti senza manuali e firmware specifici.
5. Documentare ADR 0016, playbook VS-008, traceability, risultati grezzi e diagramma transport/protocol/product.
6. **Non mergiare la PR automaticamente.** Aprire come DRAFT, fare modifiche incrementali nella stessa PR, review e regression tests. Solo quando T01–T12 con CI verde e comportamento real-time/durability corretto si potrà richiedere la promozione.

## 7. Ordine di esecuzione per Claude Code

**(a) ora:** partire dal codice presente in feature/vs008-modbus-rtu; verificare con rustfmt, clippy e test CRC/RTU; correggere eventuali errori. Aggiungere ADR 0016.
**(b) implementazione:** isolare un modulo seriale e BusArbiter un-owner-per-bus; estrarre minimo comune del catalogo TCP se necessario, senza cambiare il contratto; nuovo driver RTU e parser/--check-config.
**(c) prove:** PTY simulatore multi-slave, E2E kernel/journal reali, test di guasto e fuzz; coprire le criticità temporali USB e i late replies.
**(d) prestazioni:** campaign script e raw results, soak, CPU/RSS, ARM64 compile check e limiti field hardware espliciti.
**(e) review:** checklist test vs implementazione, CI e review indipendente. Non dichiarare PRODUCTION-READY senza qualificazione hardware, non merge automatico.

**Riferimenti esterni:** Modbus Organization, Modbus Serial Line V1.02 https://www.modbus.org/docs/Modbus_over_serial_line_V1_02.pdf ; serialport-rs API https://docs.rs/serialport/4.10.1/serialport/ .
