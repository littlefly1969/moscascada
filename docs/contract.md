# Contratto semantico (VS-001, aggiornato in VS-002, VS-003 e VS-004)

Questo documento è normativo per `scada-core` e `scada-runtime`. Schema wire: `proto/scada/driver/v1/driver.proto`. Decisioni e alternative: ADR 0001 e 0002; per VS-002 ADR 0004–0009; VS-003 ADR 0010; VS-004 ADR 0011.

**VS-002 non modifica lo schema `scada.driver.v1`** (nessun campo, messaggio o valore enum nuovo; `buf breaking` contro la baseline v1.0 è verde) né la semantica di `Kernel::apply`, di `APPLIED_VOLATILE`, della deduplicazione e della qualità. Cambiano solo due comportamenti del runtime, entrambi espressi con messaggi già esistenti (§2.1), e si aggiunge una procedura raccomandata per il driver (§5.1). Lo schema di sola diagnostica `scada.introspect.v1` riceve solo campi additivi.

**VS-004 introduce il minor 1.1** di `scada.driver.v1` (tutto additivo, `buf breaking` verde): `Hello.protocol_minor`, messaggio `Heartbeat`, `ERROR_CODE_INVALID_HEARTBEAT` (§2.3), e la supervisione opzionale di liveness e freschezza (§6.1). **A supervisione disabilitata (default) il contratto è quello VS-003**: stessi esiti, stesse qualità, stessi ACK, digest v1.

## 1. Identità

| Nome | Tipo | Chi lo assegna | Significato |
|---|---|---|---|
| `driver_id` | stringa ≤64 B | driver | Componente software che apre la connessione. Solo diagnostico. |
| `source_id` | `u32` | configurazione | Sorgente/dispositivo logico stabile. Autorizzata da config + `SO_PEERCRED`, non autenticata dal valore dichiarato. |
| `source_epoch` | `u64` ≠ 0 | produttore | Identità opaca della sessione dati del produttore; nuova dopo ogni riavvio del produttore; può restare uguale su un reconnect senza riavvio. |
| `link_generation` | `u64` | sequencer | Generazione del collegamento, crescente globalmente. Serve a ignorare eventi ritardati di link precedenti. |
| `source_sequence` | `u64` ≥ 1 | produttore | Progressivo dei batch nella `source_epoch` corrente. |
| `operation_sequence` | `u64` | sequencer, verificato dal core | Progressivo globale delle transizioni **applicate**. |
| `kernel_instance_id` | 16 byte casuali | runtime all'avvio | Nuovo a ogni avvio; un ACK di un'altra istanza non conferma nulla dell'istanza precedente. |

Relazione `driver_id` ↔ `source_id`: una connessione = una sessione logica = una `source_id`. Un driver con più sorgenti apre più connessioni (fino a `max_driver_connections`). Il core non conosce `driver_id`.

## 2. Sessione

1. Il driver apre la connessione e invia `DriverMessage{hello}` entro `handshake_timeout_ms`.
2. Il kernel verifica nell'ordine: messaggio decodificabile e di tipo `Hello` (`INVALID_HELLO` / `UNEXPECTED_MESSAGE`), major `1` presente (`UNSUPPORTED_VERSION`), `driver_id` ≤64 B, `source_epoch ≠ 0` (`INVALID_HELLO`), `source_id` configurata (`UNKNOWN_SOURCE`), UID del peer autorizzato se la sorgente ha `authorized_uids` (`UNAUTHORIZED`). In caso di errore: `HandshakeRejected` e chiusura.
3. Il sequencer applica `SourceConnected(source_id, link_generation, source_epoch)` e il kernel risponde `Welcome` con `kernel_instance_id`, versione negoziata (1.0, oppure 1.1 da VS-004: §2.3), `link_generation`, limiti, `full_state_required`, `last_accepted_source_sequence` (se esiste baseline nella stessa epoca).
4. Il driver invia `ObservationBatch`; può inviarne fino a `max_inflight_batches` senza attendere. Il kernel risponde con un `InputAck` per batch, nello stesso ordine.
5. Alla chiusura (EOF, errore, timeout di frame, kill del processo) il sequencer applica `SourceDisconnected(source_id, link_generation)`.
6. Errori di framing o di decode dopo l'handshake: `ProtocolError` e chiusura (lo stream potrebbe essere desincronizzato).

Se arriva un `Hello` per una sorgente già collegata, il nuovo link sostituisce il precedente (takeover): i batch successivi del link vecchio ricevono `REJECTED/LINK_SUPERSEDED` e la sua disconnessione è ignorata.

### 2.1 Cambiamenti di comportamento del runtime in VS-002 (compatibili)

- **Takeover (D3, rivisto dopo la review P2).** Quando il nuovo link riceve il `Welcome`, il link precedente della stessa sorgente viene chiuso **subito**, anche se è inattivo o bloccato in backpressure (coda ACK piena, coda del sequencer piena, peer che non legge): il kernel tenta, senza attendere, di scrivergli un `ProtocolError{error_code = LINK_SUPERSEDED}` e chiude. Gli ACK del vecchio link non ancora scritti in quel momento **possono andare persi** (mai inventati né anticipati): i batch corrispondenti sono applicati o rifiutati come sempre e il driver riconcilia con `Welcome.last_accepted_source_sequence` del nuovo link (§5). Lo slot di connessione è liberato entro il tempo di uno scheduling del runtime (misurato < 1 ms; limite verificato dal test: 2 s, comunque mai legato a `ack_write_timeout_ms`). In VS-001 restava aperto (e occupava uno slot di connessione) finché non inviava un altro batch. Un batch del vecchio link che corre in parallelo al takeover riceve, come prima, `REJECTED/LINK_SUPERSEDED`. In entrambi i casi nessun dato del vecchio link è applicato dopo il takeover. Un driver VS-001 gestisce già `ProtocolError` come errore fatale della connessione.
- **Peer che non legge (D2).** Ogni scrittura verso il driver (ACK, `Welcome`, rifiuti) deve completarsi entro `ack_write_timeout_ms` (default 5000 ms, `[ipc]`); altrimenti la connessione è chiusa senza ulteriori messaggi. Gli input già applicati restano applicati e i loro ACK sono persi; il driver riconcilia riconnettendosi (§5.1). Nessun ACK viene inventato o anticipato.

### 2.2 Politica di ingresso (VS-003, ADR 0010) — opzionale, default invariato

`[ipc] ingress_policy` sceglie come le richieste ammesse raggiungono l'unico writer. **Default `fifo`: comportamento VS-002 identico.** Le altre politiche non cambiano wire, core, ACK né semantica per sorgente; cambiano solo **quando** una richiesta di una sorgente è servita rispetto a quelle delle altre.

| `ingress_policy` | Ammissione | Ordine di servizio |
|---|---|---|
| `fifo` (default) | coda globale `input_queue_capacity` | ordine di ammissione |
| `source_fair` (sperimentale) | al più `per_source_pending_limit` per sorgente e `input_queue_capacity` in totale | round-robin fra le sorgenti con richieste in coda, una richiesta per turno |

Garanzie con ogni politica: (1) ordine dei batch e degli ACK di una sorgente = ordine del socket; (2) `APPLIED_VOLATILE` solo dopo `apply`; (3) `operation_sequence` = ordine effettivo di applicazione (l'ordine **fra sorgenti** non è un contratto: cambia fra politiche ed esecuzioni; è riproducibile solo come stream catturato, §8); (4) un batch che il kernel ha ammesso riceve un esito tipizzato, o la connessione viene chiusa senza ACK (peer morto, takeover, arresto) — mai un ACK inventato; (5) `SourceDisconnected` di un link è applicato dopo tutti i batch che quel link ha fatto ammettere.

Con `source_fair`, inoltre: un `Hello` è ammesso anche con le code dati sature (le richieste di controllo non consumano la quota dati) ed è servito dopo al più `per_source_pending_limit` batch già ammessi della stessa sorgente e dopo al più un turno per ogni altra sorgente attiva; un batch del vecchio link ammesso prima del `Hello` è applicato con la vecchia generazione, uno ammesso dopo è rifiutato `LINK_SUPERSEDED`. Il limite è in **turni di servizio**, non un tempo: nessuna garanzia hard real-time.

### 2.3 Minor 1.1 e heartbeat (VS-004, ADR 0011 D2–D3)

- Il driver dichiara in `Hello.protocol_minor` il minor massimo che parla (assente/0 = 1.0, driver VS-001..003). Il kernel VS-004 risponde `negotiated_protocol_minor = min(1, protocol_minor)`; un kernel VS-001..003 risponde sempre 0.
- **Regola obbligatoria del driver:** inviare `DriverMessage{heartbeat}` **solo se** `Welcome.negotiated_protocol_minor >= 1`. Un kernel 1.0 tratta un heartbeat come messaggio inatteso e chiude il link.
- `Heartbeat.sequence` ≥ 1 e strettamente crescente sul link; non è un timestamp né una prova di acquisizione. Nessuna identità: vale per la sorgente del `Hello`.
- **Nessun `InputAck`** per un heartbeat; nessun effetto su `source_sequence`, deduplicazione, ACK dei batch, `operation_sequence`.
- Heartbeat su link negoziato 1.0 → `ProtocolError{UNEXPECTED_MESSAGE}`; `sequence` 0 o non crescente → `ProtocolError{INVALID_HEARTBEAT}`; in entrambi i casi chiusura (fatale).
- Un heartbeat valido prova solo che il driver è vivo (§6.1): **non rinnova mai la freschezza né porta un tag a `Good`**. Raffiche di heartbeat sono coalescenti (al più una prova per tick del watchdog), non entrano nella coda del sequencer e non consumano memoria.
- Con supervisione disabilitata gli heartbeat di un driver 1.1 sono accettati, contati e senza effetto.

## 3. Ingresso e ACK

| Esito core | ACK | `operation_sequence` | Effetto sullo stato |
|---|---|---|---|
| `Applied` | `APPLIED_VOLATILE` + `continuity` | nuovo numero | applicato per intero in RAM |
| `Duplicate` | `DUPLICATE` | assente | nessuno |
| `Err(e)` | `REJECTED` + `error_code` | assente | nessuno (`S' == S`) |

`APPLIED_VOLATILE` viene inviato solo dopo il ritorno di `apply`. **Non esiste uno stato durevole**: nessun ACK sopravvive a crash, arresto o riavvio del kernel. Lo stato `DurableCommitted` non esiste e non è simulato.

## 4. Validazione di un `ObservationBatch` (tutto o niente)

Controlli completati prima di qualsiasi mutazione, nell'ordine:

1. `operation_sequence == last + 1` (`OperationSequenceMismatch`), `last < u64::MAX`.
2. Sorgente configurata; link attivo con la stessa `link_generation` (`LINK_SUPERSEDED` se non è il link attivo).
3. Modalità valida; 1 ≤ numero osservazioni ≤ `max_observations_per_batch` (`EMPTY_BATCH`, `BATCH_TOO_LARGE`).
4. Ogni osservazione: valore presente (`MISSING_VALUE`), canale configurato per la sorgente (`UNKNOWN_CHANNEL`), tipo del valore = tipo del tag (`TYPE_MISMATCH`).
5. Nessun canale ripetuto (`DUPLICATE_CHANNEL`).
6. `FullState`: esattamente tutti i canali configurati della sorgente (`INCOMPLETE_FULL_STATE`).
7. Regole di sequenza (§5).
8. Nessun tag che cambierebbe ha `revision == u64::MAX` (`REVISION_OVERFLOW`).

Gli aggiornamenti vengono canonizzati per `TagId` prima del confronto con l'ultimo batch e dell'applicazione.

## 5. Sequenza per sorgente

Sia `L` = ultimo `source_sequence` accettato nella stessa epoca (assente se non c'è baseline).

| Condizione | `FullState` | `Delta` |
|---|---|---|
| `seq == 0` | `INVALID_SEQUENCE` | `INVALID_SEQUENCE` |
| `L` assente | applicato, `BASELINE_ESTABLISHED` | `RESYNC_REQUIRED` |
| `seq == L+1` | applicato, `CONTINUOUS` | applicato, `CONTINUOUS` |
| `seq > L+1` | applicato, `RESYNCHRONIZED` | `SEQUENCE_GAP` (il driver deve inviare un `FullState`) |
| `seq == L`, payload canonico identico (stessa modalità) | `DUPLICATE` | `DUPLICATE` |
| `seq == L`, payload diverso | `SEQUENCE_CONFLICT` | `SEQUENCE_CONFLICT` |
| `seq < L` | `STALE_SEQUENCE` | `STALE_SEQUENCE` |
| `L == u64::MAX` e `seq != L` (contatore dell'epoca esaurito, nessun wrap: serve una nuova epoca) | `SEQUENCE_OVERFLOW` | `SEQUENCE_OVERFLOW` |

Deduplicazione: solo l'ultimo batch accettato per sorgente (copia canonica). Non c'è memoria delle epoche precedenti: un `SourceConnected` con epoca diversa azzera `L`.

`BASELINE_ESTABLISHED` e `RESYNCHRONIZED` segnalano che la continuità storica precedente **non** è dimostrata: gli eventi intermedi non sono recuperati né inventati.

### Reconnect senza perdita (procedura del driver)

1. Il driver mantiene la stessa `source_epoch` e, in memoria, i batch inviati ma non ancora confermati (al più `max_inflight_batches`).
2. Dopo il nuovo `Welcome`, se `last_accepted_source_sequence = L` è presente: scarta i batch con `seq ≤ L` (già applicati), ritrasmette quelli con `seq > L` nell'ordine originale.
3. Ritrasmettere il batch `L` stesso è sicuro: produce `DUPLICATE` senza riapplicarlo.
4. Se `full_state_required` è vero (kernel riavviato: nuovo `kernel_instance_id`, oppure epoca nuova), invia un `FullState`.
5. Dopo il reconnect i tag restano `Uncertain(SourceUnavailable)` finché una nuova osservazione valida non li aggiorna.

### 5.1 Ripresa con fotografia fresca (VS-002, D1)

Con i soli passi precedenti, un dispositivo che dopo il reconnect invia `Delta` solo per i canali che cambiano lascia gli altri tag `Uncertain` indefinitamente. Procedura raccomandata (implementata in `scada_sim_driver::resume`, senza alcuna modifica del protocollo):

1. Passi 1–3 sopra: ritrasmissione, in ordine, dei soli batch non confermati con `seq > L`.
2. Subito dopo, un `FullState` con il **valore corrente** di tutti i canali, con la sequenza successiva. Continuità `CONTINUOUS` (nessun buco) e tutti i tag della sorgente tornano `Good`, compresi quelli non cambiati.
3. Un batch ritrasmesso già applicato (`seq == L`) produce `DUPLICATE` e **non** rinfresca la qualità: un dato vecchio non diventa prova di freschezza.
4. Eventi intermedi che il dispositivo non ha conservato fra l'ultimo batch ritrasmesso e la fotografia sono persi: la fotografia sostituisce lo stato ma non li ricostruisce. Se il dispositivo ha perso anche la sequenza (nuova epoca o buco), la fotografia è `BASELINE_ESTABLISHED` o `RESYNCHRONIZED`, che dichiarano esplicitamente la discontinuità.
5. Un dispositivo che non può produrre una fotografia salta il passo 2: i suoi canali non osservati restano, correttamente, `Uncertain`.

Test: `vs002.rs::reconnection_with_partial_deltas_then_fresh_full_state`, `simulator_resume_procedure_reasserts_all_channels`, `continuous_replay_vs_sequence_loss`.

## 6. Qualità

| Evento | Tag della sorgente con valore | Tag senza valore |
|---|---|---|
| Avvio kernel | — | `Unknown`, revisione 0 |
| Osservazione valida applicata | `Good`, valore nuovo | `Good`, valore nuovo |
| `SourceDisconnected` del link attivo | `Uncertain(SourceUnavailable)`, valore preservato | resta `Unknown` |
| `SourceConnected` (anche takeover) | `Uncertain(SourceUnavailable)`, valore preservato | resta `Unknown` |
| Disconnessione di generazione vecchia | nessun effetto | nessun effetto |

Un valore `fault=true` correttamente ricevuto è un dato `Good`: la qualità descrive la comunicazione, non lo stato fisico. Nessun timeout nel core: da VS-004 le scadenze entrano come operazioni sequenziate decise dal runtime (§6.1).

### 6.1 Supervisione opzionale (VS-004, ADR 0011) — disabilitata per default

Per sorgente: `liveness_timeout_ms` e/o `freshness_timeout_ms` in `[[sources]]`; `[runtime] watchdog_poll_ms` (default 250, 10..=60000). Ogni timeout `>= 3 × watchdog_poll_ms`, `<= 86 400 000`, `freshness / poll <= 4096`; configurazioni incoerenti rifiutate all'avvio.

- **Liveness.** Prova di vita = heartbeat valido del link corrente o batch **applicato** (non `Duplicate`, non rifiutato). Nessuna prova per `>= liveness_timeout` ⇒ operazione `SourceSilenceExpired`: sorgente `Silent`, **link ancora `Connected`** (nessun `SourceDisconnected` fittizio), tag con valore → `Uncertain(SourceSilent)`, valore preservato. Un heartbeat successivo ⇒ `SourceLivenessRestored`: sorgente `Alive`, **nessun tag cambia**. Un batch applicato rende la sorgente `Alive` e `Good` solo i tag che contiene.
- **Freschezza per tag.** Un batch applicato rinnova ogni tag che contiene (anche con valore identico; `FullState` = tutti, `Delta` = solo i presenti). `Duplicate`, `REJECTED`, malformati, heartbeat: nessun rinnovo. Un tag `Good` non rinnovato da `>= freshness_timeout` ⇒ `Uncertain(StaleData)`, valore preservato, revisione +1 solo su questo primo cambio. Un tag mai osservato resta `Unknown`.
- **Precedenza:** `SourceUnavailable` > `SourceSilent` > `StaleData`; una degradazione si applica solo verso un rango strettamente maggiore; **solo un'osservazione applicata porta `Good`**.
- **Precisione:** una scadenza è rilevata fra `T` e `T + watchdog_poll_ms + ritardo del tick`, **mai prima di `T`**. Nessuna garanzia hard real-time. «Freschezza» è il tempo dall'ultima osservazione nuova accettata dal kernel, non l'età fisica della misura nel dispositivo.
- **Determinismo:** il core non legge clock. Le scadenze sono operazioni con guardie verificabili senza tempo (generazione del link; token `observed_up_to` in operation sequence) e consumano un `operation_sequence` solo se applicate; quelle obsolete (link sostituito, già silente, nulla da far scadere) sono no-op. Con supervisione attiva il digest è **v2** (aggiunge `silent`, `last_batch_operation`, marker per tag), senza supervisione resta **v1**.

| Evento | Tag `Good` | Tag `Uncertain(StaleData)` | Tag `Uncertain(SourceSilent)` | Tag `Unknown` |
|---|---|---|---|---|
| Osservazione applicata del tag | `Good` | `Good` | `Good` | `Good` |
| `FreshnessExpired` (tag non osservato da ≥ T) | `Uncertain(StaleData)` | invariato | invariato | invariato |
| `SourceSilenceExpired` | `Uncertain(SourceSilent)` | `Uncertain(SourceSilent)` | invariato | invariato |
| Heartbeat / `SourceLivenessRestored` | invariato | invariato | invariato | invariato |
| `SourceDisconnected` / `SourceConnected` | `Uncertain(SourceUnavailable)` | `Uncertain(SourceUnavailable)` | `Uncertain(SourceUnavailable)` | invariato |

Revisione: +1 a ogni cambio di `(value, quality)`; nessun wrap (`REVISION_OVERFLOW`).

## 7. Riavvio del kernel

Un nuovo processo kernel genera un nuovo `kernel_instance_id`, parte con tutti i tag `Unknown` e nessuna baseline: il `Welcome` riporta `full_state_required = true` e nessun `last_accepted_source_sequence`. Un driver che vede cambiare `kernel_instance_id` deve considerare persi tutti gli ACK precedenti.

## 8. Invarianti verificati dai test

- I1 Atomicità: `apply` → `Err` ⇒ snapshot e digest invariati.
- I2 Determinismo: due kernel con stessa config e stessi input ⇒ stessi snapshot/digest a ogni passo.
- I3 `operation_sequence` cresce di 1 solo con `Applied`.
- I4 Le revisioni non diminuiscono mai; cambiano solo se cambia `(value, quality)`.
- I5 Nessuna struttura del core cresce oltre la capacità iniziale.
- I6 Ordine di applicazione dei tag e digest indipendenti dall'ordine delle osservazioni nel batch.
- I7 (VS-002) Equivalenza: il `Kernel::apply` ottimizzato dà lo stesso esito e lo stesso snapshot logico della copia congelata di VS-001 (`tests/reference/vs001_kernel.rs`) su ogni input (`tests/equivalence.rs`, e sugli stream catturati da kernel reali in `vs002.rs`).
- I8 (VS-002) Per ogni sorgente, ACK nello stesso ordine dei batch e `operation_sequence` strettamente crescenti, con qualunque scheduler; nessun `operation_sequence` assegnato due volte.
- I9 (VS-004) Senza supervisione: digest/fingerprint v1 byte-identici, operazioni di supervisione rifiutate (`SupervisionNotConfigured`).
- I10 (VS-004) Solo un batch applicato porta un tag a `Good`; heartbeat e `SourceLivenessRestored` non cambiano nessun tag; `FreshnessExpired` cambia solo tag `Good` con marker `<=` token.
- I11 (VS-004) Le operazioni di supervisione obsolete o ripetute sono no-op (stato, digest, `operation_sequence` invariati); il replay di uno stream catturato con scadenze riproduce lo stesso digest senza clock.
