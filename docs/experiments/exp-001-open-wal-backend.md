# EXP-001 — Spike open-wal: journal binario come backend sperimentale di MoScada

**Stato: SPECIFICA DI ESPERIMENTO / DRAFT.** Esito dello spike: [`exp-001-report.md`](exp-001-report.md) (raccomandazione: mantenere SQLite). Questo documento avvia il lavoro; non costituisce implementazione, benchmark o prova di durabilità. Non rilasciare in produzione. La PR parte dal commit VS-006 3a409caca6cf8ca8793233c2bb2e931e1f3cfc21 e ha temporaneamente come base il branch della PR #6, ancora non mergiata. **Non mergiare PR #6 o questa PR automaticamente.**

## 0. Missione e confini

Confrontare il journal SQLite WAL/FULL di VS-006 con un journal binario append-only basato su **open-wal 0.2.1** (https://docs.rs/open-wal/0.2.1/open_wal/ e https://guyo13.github.io/open-wal/), sulle medesime operazioni e con prove di correttezza, durabilità e recovery. Il candidato deve poter diventare un backend del solo **journal autorevole**: non pretendere di sostituire con open-wal l'historian SQL, gli indici e le API di interrogazione.

Perimetro vincolante:
- Lasciare completamente invariati scada-core::Kernel::apply e il sequencer single-writer, quando possibile. Nessuna variazione a driver protocol v1.2, logica allarmi, ACK, UDS, qualità, Time Authority, ordinamento, digest/default-off e pubbliche API VS-006.
- Mantenere SQLite come default e baseline di produzione. open-wal solo in una variante sperimentale esplicita, preferibilmente feature-gated o inizialmente confinata al benchmark harness, senza caricare il nuovo backend a runtime nelle build ordinarie.
- Conservare esattamente le semantiche di **admission prima di apply, zero perdita silenziosa, all-or-nothing per ogni operazione, commit watermark reale, retry/fault policy, replay/corruzione fail-closed e idempotenza di ACK**.
- Operazioni e allarmi non si riordinano per orari UTC. Conservare tutti i timestamp: device, driver, received, sequenced, logical, durable (se disponibile), clock quality/provenance e anomalie; non alterare mai i timestamp originali della centrale.
- Conservare nel recovery: RTDB, allarmi, occurrence/activation token, ACK, proiezioni/flag aggiornati senza evento, gap di continuità, source epochs/sequences, recovery fence, clock state e timeline/config fingerprint. Il recuperato resta unverified fino a FullState; replay non genera nuove notifiche o comandi.
- Non cambiare/migrare automaticamente la timeline SQLite VS-006, non attivare online un dual authoritative writer, non introdurre database esterno obbligatorio.

## 1. Due livelli, non uno scambio diretto di database

SQLite oggi svolge **due ruoli**: (a) journal autorevole con commit durevole e replay, (b) proiezioni/indici/Historian. open-wal copre **solo (a)**, con sequenze LSN e record opachi. Quindi creare confronti distinti:

- **A — SQLite completo VS-006:** journal + aggiornamento SQL delle proiezioni nella transazione, historian pronto; baseline di correttezza/end-to-end.
- **B — open-wal solo journal:** un record durevole per operazione, replay del kernel; throughput ceiling del log senza indici. Esplicitamente **NON confrontabile come sistema completo** con A.
- **C — open-wal + materializzatore SQL:** il log è l'unica autorità; una proiezione derivata SQLite nel profilo Micro o PostgreSQL nel profilo Enterprise; misurare lag di materializzazione, costo CPU/RAM/disco, query e recupero dell'indice. **Solo A vs C** risponde alla domanda sull'intercambiabilità applicativa, confrontando gli stessi dati e garanzie.
- **D — PostgreSQL remoto** opzionale in una fase successiva: la replica non decide il watermark di durabilità locale. Il backend di storico è un'ulteriore scelta, non un parametro del journal.

Il materializzatore deve essere idempotente su (timeline_id, operation_seq) e transazionale sui batch SQL; tenere un suo checkpoint/watermark durabile **distinto** dal watermark del WAL. Dopo crash o perdita dell'indice, rigenerare la proiezione dal WAL/checkpoint. Una lettura deve dichiarare esplicitamente l'operazione durevole alla quale è aggiornata; non spacciarla per lo stato live se ha lag.

## 2. Verificare open-wal prima di integrarlo

1. Bloccare una versione precisa di open-wal e il relativo Cargo.lock, verificare licenza, advisories, upstream activity, MSRV e dipendenze. Evitare versioni flottanti e vendoring arbitrario.
2. Verificare API effettive da sorgente e test: Wal::open, append, commit, durable_lsn, reader_from, segment_size, max_record_size, checkpoint e failure handling; documentare ciò che la crate garantisce e ciò che spetta a MoScada.
3. Non confondere **group commit** con transazione: open-wal può rendere durevole solo un prefisso di record del gruppo se avviene un crash al passaggio di segmento. La nostra unità atomica deve essere **una intera operazione applicata in un solo record binario**; non dividere mai una singola operazione su record differenti.
4. Documentare il limite di dimensione del record: il default della crate è 1 MiB. Un'operazione con fan-out molto alto può eccederlo. Configurare il limite oppure rifiutare prima di Kernel::apply, con errore tipizzato e nessuna mutazione; vietato troncare gli eventi o suddividere l'operazione senza protocollo transazionale esplicito.
5. Se append, write o fdatasync falliscono e la crate avvelena il writer, entrare in **FAULT fail-closed**. Non continuare con watermark presunto né riconoscere ACK. Riavvio e recovery prima di riprendere.
6. Verificare la semantica del checkpoint e del delete-segment: vietato cancellare un segmento ancora necessario a recovery, checkpoint valido, historian/replica arretrata o verifiche di integrità. Lo spike può inizialmente disabilitare del tutto la retention: un log pieno deve applicare backpressure, non eliminare dati.

## 3. Contratto minimo di backend / isolamento del cambiamento

Individuare un punto di separazione in scada-journal fra **durable append log** e **materialized historian**; usare un adapter minimo, NON creare una mega-astrazione generica e NON duplicare il kernel. Proporre PR incrementalmente:

- Fase 1, **bench-only**: adapter experimental_open_wal per una rappresentazione di OpRecord già normalizzata; stesso input canonico dei benchmark VS-006. Non toccare il runtime; mettere la crate dietro feature opt-in.
- Fase 2, **replay verificato**: serializzare in un unico envelope versionato l'intera operazione con tutti i metadati. Implementare checksum/bounds e riconciliare l'event_seq prodotto in replay con quello archiviato. Validare fingerprint, timeline, sequenza consecutiva, schema version, record LSN→op mapping e checkpoint.
- Fase 3, **materializzazione derivata**: riutilizzare più codice possibile del codec, schema SQL e historian VS-006; niente SQL nell'ack critical path del WAL. Proiezioni, operator actions, time annotations e source recovery devono poter essere rigenerate. Testare cursor e pagine degli allarmi, incluso input_valid aggiornato senza evento.
- Fase 4, **eventuale prova runtime isolata**: solo quando la fase 2/3 è verde, consentire selezione sperimentale tramite opzione esplicita; SQLite rimane default. Il journal non deve dichiarare durable finché open-wal commit() non ritorna il LSN corrispondente. Il replay dei record non deve riconsumare tempo di parete.

Il payload del solo Operation di VS-006 non basta: per un singolo record occorre preservare oltre all'operazione: operation_seq, eventi, event_seq, origins, alarm_updates anche senza evento, TimeEnvelope + SourceTimes, actor/action_id/note/outcome, recovery updates, anomalie, config fingerprint e versione. Preferire codec deterministico con header length+version+CRC o checksum, senza heap non bounded per dati ostili; definire il mapping LSN↔operation_seq quando esistono marker/checkpoint.

## 4. Test di correttezza (obbligatori prima di conclusioni prestazionali)

- **T01** confronto applicazione di uno stream canonico comune: A vs B/C producono identici state_digest, historian alarm event sequence, episodi, ACK, quality e watermark logico a pari prefisso.
- **T02** arresto corretto, crash durante append (non durevole), crash dopo commit, crash prima della pubblicazione del watermark: nessun ACK prematuro; recupero coerente, no event duplication.
- **T03** crash a ogni fault point, random SIGKILL, torn tail, truncation, bit flips, corruption in the middle, checksum mismatch, checkpoint danneggiato, recovery fail-closed.
- **T04** fsync EIO, segment roll boundary durante batch di commit, prefisso parzialmente durevole (non trattarlo come transazione del gruppo), writer poisoned, disk full e segment preallocation failure.
- **T05** burst 0/1/10/100/1k/10k eventi per operazione, evento singolo > default max_record_size, massima configurazione validata, tutti i limiti byte/operazioni/disco con risposta tipizzata **prima** di apply.
- **T06** timeout slow disk e WAL stalled: ring mai sovrascritto, receipts durevoli attendono, backpressure; reconnect e DUPLICATE non prematuro anche senza supervisione (fix PR #6).
- **T07** ACK idempotente tramite action_id, stesso token, lost reply, retry dopo reboot, nuova occurrence e token stale; storico e projection rimangono coerenti.
- **T08** recovery fence conserva latch, marca input invalid, no evento inventato; proiezione SQL derivata riflette i cambi di qualità **senza evento** (fix PR #6), dopo perdita/sincronizzazione della proiezione.
- **T09** clock -1 s/-1 h/+24 h, UTC e DST Europe/Rome; LSN/op/event_seq causalmente monotoni, source device timestamp raw invariati e qualità clock mantenuta.
- **T10** checkpoint/retention con log enorme, milioni di operazioni, segno dell'incompletezza del consumer: mai cancellare dati per historian o replica arretrata; replay entro tempo misurato.
- **T11** fuzz/property dei record, lunghezze, versioni, payload corrotti, parser e scanner; stessa politica di allocazioni bounded di VS-006.
- **T12** cross-platform x86_64 + aarch64 (almeno cargo check completo, compresa la nuova crate); su Micro hardware test di power loss reale prima di ipotizzare la promozione a produzione.

## 5. Benchmark: matrice controllata e risultati grezzi

Eseguire A/B/C sulla **stessa macchina**, stesso filesystem (e tmpfs solo come diagnostica), stessi kernel/driver, stessa config, stessi batch e regole, stessa durata, stesso volume di dati, stessa versione Rust, stessa policy fsync e stesso load di lettura. Confrontare separatamente:
- journal-only input → durable watermark, con commit batch 1/16/128/1024 e delay 0/1/5 ms; op/s, event/s, osservazioni/s, MiB/s, write amplification, byte/evento, fsync/s, tempo durable p50/p99/p99.9, CPU per fase;
- intero E2E con 1/8/16 driver × 1/128 osservazioni, ingress FIFO/source_fair, con e senza allarmi e transizioni, real durable receipts, storico interrogato in parallelo;
- 10 M eventi / historian pages 1000, richieste miste, backlog projector, recovery da checkpoint/fallback e rebuild dell'indice;
- memory RSS/picco/CPU/IOPS e page faults, ogni buffer/coda con byte bound; cold start, soak ≥30 min **di acquisizione continua** (spazio disco sufficiente), DISK_FULL e recovery;
- misurare la latenza di fsync reale; le misure sulla VM non sono certificazione power loss. Riportare mediana, min-max, p99 e p99.9 con 10 ripetizioni per cella critica e varianza/rumore.
- Non presentare B journal-only come “X volte più veloce” rispetto ad A completo. Se C è più lento o la lettura vede lag, includerlo esplicitamente nella decisione.

Baseline pubblicata VS-006 (VM 4vCPU, storage virtuale, NON ripetuta in questo spike): journal SQLite FULL saturato ~17–21k batch/s (8/16×128), ~194–230k eventi/s storm; receipt stop-and-wait ~1500–1750 batch/s. Questi valori sono di riferimento storico, non un confronto valido senza nuova campagna in ambiente identico.

## 6. Decisione Micro / Compact / Enterprise e interoperabilità

- **Micro**: se non c'è miglioramento netto end-to-end e semplificazione, preferire SQLite solo. open-wal + projector SQLite può costare più RAM/disco di SQLite integrato; misurarlo prima.
- **Server**: valutare open-wal autorevole + SQLite projector locale con read load; allocazione CPU/RAM tramite quote e affinity soltanto se misurate.
- **Enterprise**: open-wal autorevole + PostgreSQL projector remoto opzionale; replica con checkpoint idempotente e watermark REMOTO distinto. In assenza di rete continuare nel limite disco locale e dichiarare lag; non dichiarare durabilità remota con ACK locale.
- **Futura selezione**: distinguere configurazione **journal_backend** dalla configurazione **historian_backend**; non confonderle in un parametro. Nessuna pretesa di migrazione diretta fra un file .db SQLite e segmenti WAL; migrazione offline versionata con rollback e verifiche in fase successiva.

## 7. Deliverable e criterio di uscita

La PR sperimentale deve contenere:
1. codice adapter feature-gated e isolamento della variante; nessuna modifica funzionale di default-off o SQLite;
2. ADR/spike report su unità atomica, formato, mapping LSN→op/event_seq, commit/watermark, retention/poison/fault;
3. tests T01–T12, log di esecuzione e CI separata; fuzz per decoder e dischi corrotti;
4. script riproducibile, CSV grezzi, ambiente hardware/software, tabelle A/B/C e risultati su x86_64 + ARM64/Micro se disponibile;
5. profilo di capacità e backlog per Micro e Server, indicazione esplicita dei limiti e cosa resta non provato;
6. **raccomandazione motivata**: mantenere SQLite / approfondire open-wal / scartare open-wal.

**Gate:** non promuovere a produzione né mergiare PR sperimentale senza review indipendente. Dare preferenza a open-wal solo con miglioramento *ripetibile* **end-to-end A vs C**, latenza/capacità entro SLO e **nessuna perdita delle invarianti di VS-006**; non basta un benchmark di append. Anche uno spike inconcludente è un risultato corretto se misurato e documentato.

## 8. Istruzione esecutiva per l'agente

Lavorare in questa PR draft, mantenendo il branch della PR #6 come base fino al suo eventuale merge autorizzato. Partire dall'implementazione reale di VS-006 e dai test esistenti; procedere per fasi e committare i risultati. NON MERGIARE PR #6 e NON MERGIARE QUESTA PR. Non inventare misure né dichiarare implementato ciò che è soltanto previsto nella presente specifica.
