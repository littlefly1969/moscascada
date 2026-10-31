# ADR 0008 — Liveness del writer, takeover e ripresa dopo riconnessione (D1–D3)

- **Stato:** accettato (VS-002)
- **Data:** 2026-10-31
- **Ambito:** `scada-runtime::server` (writer, reader), `scada-runtime::sequencer` (link attivi), `scada-sim-driver::resume`, `docs/contract.md` §2.1 e §5.1. Wire protocol `scada.driver.v1` **invariato**.

## Contesto

Problemi individuati in VS-001 (brief §6):

1. **D2.** `writer_task` faceva `write_all` degli ACK senza scadenza. Un driver che non legge mai le risposte riempie il buffer del socket; il writer resta sospeso per sempre, lo slot di connessione (`max_driver_connections`) e le strutture della sessione restano occupati. Gli input già sottomessi sono applicati, ma il driver non lo saprà mai su quella connessione.
2. **D3.** Al takeover (nuovo `Hello` della stessa sorgente) il sequencer incrementava `link_generation`; il vecchio link riceveva `LINK_SUPERSEDED` solo **se e quando** inviava un altro batch. Un vecchio link inattivo (driver bloccato, rete mezza aperta) teneva uno slot indefinitamente: dopo `max_driver_connections` takeover consecutivi nessuna sorgente poteva più connettersi.
3. **D1.** `SourceConnected` rende `Uncertain(SourceUnavailable)` i tag della sorgente; i delta successivi aggiornano solo i canali cambiati, quindi quelli invariati possono restare `Uncertain` indefinitamente anche se la sorgente è viva e il valore non è cambiato.

## Decisioni

### D2 — scadenza di scrittura degli ACK

Ogni `write` del writer (ACK, eventuale gruppo di ACK già pronti, `Welcome`) è eseguita con `tokio::time::timeout(ack_write_timeout_ms)`. Allo scadere la sessione è chiusa: contatore `ack_write_timeouts`, slot rilasciato, `SourceDisconnected` sequenziato dopo i batch sottomessi (come per ogni chiusura).

- Default 5000 ms, configurabile in `[limits] ack_write_timeout_ms` (1..=600000).
- Gli input già applicati restano applicati (lo stato è quello del kernel, l'ACK è solo una notifica). **Nessun ACK è inventato** né riemesso altrove; il driver riconcilia alla riconnessione con `Welcome.last_accepted_source_sequence` e la deduplicazione per sequenza (contratto §5).
- Alternative scartate: (a) timeout sul *flusso* complessivo (più semplice ma chiude anche driver lenti che leggono), (b) scartare ACK quando il buffer è pieno (perdita silenziosa di risposte, vietata), (c) coda ACK illimitata (vietata). La coda ACK resta limitata a `max_inflight_per_connection`: quando è piena il reader smette di leggere, quindi la memoria della sessione è limitata anche prima della scadenza.
- Test: `vs002.rs::non_reading_peer_is_closed_and_reconciles_by_reconnect` (peer che invia fino a riempire i buffer senza leggere; chiusura entro scadenza + margine; contatore; riconnessione con `last_accepted_source_sequence` coerente con lo stato).

### D3 — chiusura mirata del link superato

Il sequencer conserva, per ogni sorgente, la generazione attiva e un `oneshot::Sender<()>` della sessione che la possiede (`Links.active`). Quando un nuovo `SourceConnected` viene applicato e il `Welcome` è prodotto, il sequencer **sostituisce** l'elemento e invia `()` al vecchio link. Il reader del vecchio link è in `select!` anche su quel ricevitore: termina inviando `ProtocolError LINK_SUPERSEDED` e chiude la connessione (contatore `links_superseded`).

- Nessun lock, nessuno stato condiviso nuovo: è un messaggio prodotto dall'unico writer, nello stesso ordine in cui la generazione è stata assegnata. Il core non cambia: i batch eventualmente già in coda del vecchio link sono ancora rifiutati da `Kernel::apply` (`LINK_SUPERSEDED`) e la sua `SourceDisconnected` tardiva è `Ignored(StaleLinkGeneration)` — non degrada il nuovo link.
- **Cambio di comportamento osservabile** (contratto §2.1): prima un link superato poteva restare aperto fino al batch successivo; ora viene chiuso. Il codice di errore è lo stesso già esistente in `scada.driver.v1`, quindi nessuna modifica di wire protocol. Un driver conforme a VS-001 non deve cambiare nulla: il link superato era già inutilizzabile.
- Alternativa scartata: timeout di inattività sui link (chiude anche link legittimi silenziosi, introduce una politica di tempo che il contratto non prevede).
- Test: `vs002.rs::repeated_takeovers_do_not_exhaust_connection_slots` (`max_driver_connections` × 3 takeover con vecchi link inattivi; ogni nuova connessione accettata; contatore = numero di takeover); `ipc.rs::superseded_link_cannot_overwrite_new_generation` adattato.

### D1 — procedura di ripresa con fotografia fresca

Implementata **lato driver e contratto**, senza modifiche al wire protocol né al core (brief: "preferirlo"). `scada_sim_driver::resume`:

1. dopo il `Welcome`, reinvia in ordine i batch non confermati con `source_sequence > last_accepted_source_sequence` (quelli `≤` sono già applicati: non vengono reinviati e, se lo fossero, sono `DUPLICATE` senza effetti — **un vecchio duplicato non diventa prova di freschezza**);
2. invia **una fotografia corrente completa** (`FULL_STATE`) con una sequenza **nuova** (`next_sequence`), che riafferma tutti i canali, anche quelli non cambiati, rendendoli `Good`.

Un dispositivo che non può fornire una fotografia completa invia solo delta: i canali non osservati restano correttamente `Uncertain(SourceUnavailable)`. Gli eventi intermedi accaduti durante la disconnessione e non bufferizzati dal driver **non sono ricostruibili**: la fotografia ripristina lo stato corrente, non la storia (contratto §5.1).

Test: `reconnection_with_partial_deltas_then_fresh_full_state` (delta parziali → non osservati `Uncertain`; `FULL_STATE` fresco → tutti `Good`), `simulator_resume_procedure_reasserts_all_channels`, `continuous_replay_vs_sequence_loss` (replay continuo = nessun gap; salto di sequenza = `SequenceGap` contato, nessuna transizione inventata).

### D4/D5 — ordine e replay

Non sono decisioni di progetto nuove ma prove: vedi `docs/architecture.md` §"Ordine e concorrenza" e ADR 0004. La cattura (`--capture-inputs`) è diagnostica, non un journal (nessuna durabilità).

### D6

Nessun timeout del dispositivo dentro `Kernel::apply`; nessun motore di allarmi. Una futura supervisione dovrà produrre input sequenziati.

## Conseguenze e limiti

- La scadenza D2 chiude anche un driver che legge, ma più lentamente di quanto il buffer socket assorba per `ack_write_timeout_ms`: è voluto (risorse limitate) e configurabile.
- Fra applicazione e ACK perso non c'è alcuna garanzia di consegna; la riconciliazione è per stato (sequenza accettata), non per messaggio.
- La chiusura D3 è asincrona: per un breve intervallo il vecchio link può ancora essere aperto; i suoi batch sono comunque rifiutati dal core.
