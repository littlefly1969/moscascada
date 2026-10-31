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
- Test: `vs002.rs::repeated_takeovers_do_not_exhaust_connection_slots` (`max_driver_connections = 2`, 30 takeover consecutivi con i vecchi socket tenuti aperti dal client; nessun rifiuto per limite, `links_superseded = 29`, le chiusure tardive non degradano il link attivo); `ipc.rs::superseded_link_cannot_overwrite_new_generation` adattato.

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

### Revisione P2 — takeover di un link in backpressure

**Problema (review della PR #2).** Il segnale di takeover era osservato dal reader solo mentre aspettava un frame. Un vecchio link che produce input senza leggere gli ACK porta il reader a bloccarsi su `pending_tx.reserve()` (coda ACK piena), su `submit()` (coda del sequencer piena) o su `pending_tx.send(Fatal)`, e il writer su una `write` verso un peer che non legge. In quello stato il segnale non veniva visto: lo slot restava occupato fino alla scadenza D2 (`ack_write_timeout_ms`, default 5 s; nel test 20 s) e, con `max_driver_connections` basso, il takeover successivo veniva rifiutato.

**Riproduzione.** `vs002.rs::takeover_of_a_backpressured_non_reading_link_releases_its_slot_promptly`: `max_driver_connections = 2`, `ack_write_timeout_ms = 20000`; il vecchio client invia `Delta` senza leggere finché le sue scritture si bloccano (pipeline piena fino al socket), poi la stessa sorgente si ricollega; 3 takeover consecutivi, in `current_thread` e in `multi_thread(2)`. Prima della correzione: `links_superseded = 0`, `active_connections = 2` oltre il limite di attesa (test fallito).

**Decisione.**

1. Ogni attesa del reader (lettura del frame, `reserve`, `submit`, ogni `send` sulla coda ACK) è in `select!` con il segnale di takeover. Tutte le operazioni interrotte sono cancel-safe: un `send`/`reserve`/`submit` cancellato non accoda nulla, quindi non nasce nessun ACK né richiesta parziale.
2. Al takeover il reader incrementa `links_superseded` e invia un segnale di abort al writer (`oneshot`). Il writer interrompe subito qualunque attesa (ACK in sospeso, `write` bloccata), tenta **una sola** scrittura non bloccante (`try_write`) di `ProtocolError LINK_SUPERSEDED` e chiude il socket (shutdown con la stessa scadenza di scrittura, poi drop).
3. Gli ACK del vecchio link non ancora scritti sono scartati. Non è una perdita silenziosa di stato: i batch già sottomessi sono applicati (o rifiutati) dal sequencer nell'ordine di sempre e i successivi alla nuova generazione sono rifiutati dal core (`LINK_SUPERSEDED`); il driver riconcilia per stato con `Welcome.last_accepted_source_sequence` (contratto §2.1, §5). Era già il comportamento di D2 per un peer che non legge.

**Risultato.** Rilascio dello slot dopo il `Hello` di takeover: 0,7–1,1 ms in `current_thread`, 0,5–0,8 ms in `multi_thread(2)` (6 takeover, output del test); il test richiede < 2 s e `ack_write_timeouts = 0` (lo slot è liberato dal takeover, non dalla scadenza). Verificati anche: nessun ACK inventato (il valore del tag corrisponde a `last_accepted_source_sequence` riportato dal nuovo `Welcome`), continuità `Continuous` sul nuovo link, chiusure tardive dei vecchi socket senza effetti sul link attivo (`Connected`, qualità `Good`). `repeated_takeovers_do_not_exhaust_connection_slots` e `ipc.rs::superseded_link_cannot_overwrite_new_generation` restano verdi.

**Alternativa scartata.** Mandare `LINK_SUPERSEDED` attraverso la coda ACK dopo gli ACK pendenti (comportamento precedente): richiede che il peer legga, cioè esattamente ciò che un peer in backpressure non fa.

## Conseguenze e limiti

- La scadenza D2 chiude anche un driver che legge, ma più lentamente di quanto il buffer socket assorba per `ack_write_timeout_ms`: è voluto (risorse limitate) e configurabile.
- Fra applicazione e ACK perso non c'è alcuna garanzia di consegna; la riconciliazione è per stato (sequenza accettata), non per messaggio.
- La chiusura D3 è asincrona: per un breve intervallo (misurato < 1 ms) il vecchio link può ancora essere aperto; i suoi batch sono comunque rifiutati dal core.
- Dopo il takeover il vecchio link può non ricevere gli ACK dei batch già sottomessi né il `ProtocolError` (scrittura non bloccante, best effort): la riconciliazione è sempre per sequenza accettata.
