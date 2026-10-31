# ADR 0010 — Ingresso fair: quote per sorgente e servizio round-robin davanti all'unico writer

- **Stato:** **deciso dopo misura** (VS-003). §1–§8 sono la progettazione scritta *prima* dell'implementazione (invariata salvo note); §11 è la decisione presa sulla campagna. Esito: **FIFO resta il default**; `source_fair` (C) resta come opzione esplicita **sperimentale**; B misurata e non tenuta; D non implementata.
- **Data:** 2026-10-31
- **Ambito:** `scada-runtime::{server, sequencer, config, budget}`, nuovo crate puro `scada-sched`. `scada-core` **invariato**. Wire `scada.driver.v1` **invariato**.
- **Base:** progettazione scritta su `fd5e992`; implementazione e campagna su VS-002 finale `d9502c1` (P1/P2, CI verde), poi riallineata su `main` `9d97882` (merge di #1 e #2, stesso albero di `d9502c1`).

## 1. Contesto e problema

Il sequencer preleva da **una** `mpsc` bounded (`input_queue_capacity`) in ordine FIFO. Le sessioni che trovano la coda piena attendono nel semaforo FIFO della `mpsc`. Una sorgente rara che invia 1 osservazione arriva quindi dietro:

1. fino a `input_queue_capacity` richieste già in coda, più
2. fino a un batch per ogni sessione già in attesa nel semaforo (ogni sessione ne tiene al più uno decodificato).

Con 16 sorgenti ad alto volume (pipeline 32) la profondità per sorgente non ha limite proprio: è limitata solo da `max_inflight_per_connection` e dalla coda globale. Il problema è di **ammissione e ordine di servizio**, non solo di costo di `Kernel::apply` (brief §1).

**Misura preliminare (orientativa, su `fd5e992`, non baseline):** §9. Mostra che sulla VM di sviluppo la coda del sequencer è *una parte* del p99 della rara, non tutto: il resto è scheduling dei task di I/O (risposta pronta ma writer non ancora eseguito) e contesa CPU fra client e kernel sugli stessi 4 vCPU. Questo limita a priori il guadagno ottenibile da qualunque politica di servizio del sequencer.

## 2. Vincoli (dal brief §2, ripresi come invarianti verificabili)

| # | Invariante | Dove si verifica |
|---|---|---|
| I1 | Un solo thread chiama `Kernel::apply`; `apply` resta puro | struttura del codice (solo `sequencer::apply`) |
| I2 | `operation_sequence` = ordine effettivo di commit; rifiutati/duplicati non la consumano | core invariato + test di replay |
| I3 | Per sorgente e generazione: ordine dei batch e degli ACK preservato | `scada-sched` proptest (FIFO per lane) + test e2e |
| I4 | Nessun ACK prima di `apply`; nessun ACK inventato | writer invariato |
| I5 | Ogni batch posseduto dal kernel ha un esito tipizzato o la sessione viene chiusa senza esito (peer morto / shutdown) | test di cancellazione |
| I6 | Tutte le code limitate da configurazione; bound `u128` in `budget.rs` | `--check-config`, test budget |
| I7 | Nessun drop/coalescenza di osservazioni | nessun percorso di scarto nel codice |
| I8 | Replay dello stream catturato → stesso digest, con qualunque politica | test di cattura/replay sotto entrambe le politiche |
| I9 | Default = FIFO VS-002; la nuova politica solo esplicita | test di configurazione |

## 3. Alternative

| | Ammissione | Ordine di servizio | Cosa garantisce alla rara | Costo |
|---|---|---|---|---|
| **A** FIFO (VS-002) | `mpsc` globale | FIFO globale | attesa ≤ `Q` + sessioni in attesa | nessuno |
| **B** quote + FIFO | semaforo per sorgente (cap `c`) → `mpsc` globale | FIFO globale | attesa ≤ min(`Q`, `S·c`) + sessioni in attesa | 1 semaforo/sorgente, 1 permit per richiesta |
| **C** lane + RR | semaforo per sorgente (cap `c`) → semaforo globale `Q` | round-robin fra lane non vuote, quantum 1 | attesa ≤ (sessioni in attesa globale) + (lane attive − 1) servizi | B + `Mutex<FairQueue>` + `Condvar` |
| **D** lane + DRR | come C | deficit round-robin su costo = f(oss.) | come C, in crediti di costo | C + contatore di deficit/lane |

`S` = sorgenti con richieste in coda. **B non è fair scheduling**: limita quanto ogni sorgente può tenere davanti alla rara, ma l'ordine resta quello di arrivo. **C** rende l'attesa della rara indipendente dalla *profondità* dei backlog altrui (dipende solo dal *numero* di sorgenti attive). **D** serve solo se batch da 1 e da 128 osservazioni producono uno squilibrio misurato; DRR dà equità di servizio, non deadline (Shreedhar & Varghese 1996).

Scartate a priori (brief §2/§3): priorità assoluta (può affamare), classificazione implicita per frequenza, drop-oldest/latest-only, coda illimitata, più writer, drain del sequencer (già misurato e rimosso, ADR 0005).

### 3.1 Modello puro (`crates/scada-sched`)

`FairQueue<T>`: una lane FIFO limitata per sorgente + un anello *ready* preallocato (capacità = numero di lane) che contiene ogni lane non vuota **al più una volta** (flag `queued`); `pop` O(1), nessuna scansione di tutte le sorgenti; nessuna allocazione dopo `new` (tutte le `VecDeque` nascono con la capacità finale). Nessun Tokio, clock, I/O, lock o `unsafe`. Il runtime lo possiederà dietro la propria sincronizzazione.

Prove (`crates/scada-sched/tests/model.rs`, proptest 512–1024 casi, oracle naive O(lane)):

- ordine di servizio identico all'oracle per RR (quantum 1–4) e DRR;
- invarianti interne dopo ogni operazione, `len ≤ lanes·(cap + ctl)`, presenza nell'anello ≤ 1;
- nessuna perdita, nessun doppio servizio, FIFO per lane, `drain` (shutdown) restituisce tutto in ordine;
- **fair progress**: dal momento in cui una richiesta diventa testa della propria lane, al più `(lanes−1)·quantum` richieste di altre lane (RR) o `(lanes−1)·(quantum+max_cost)` crediti (DRR) sono serviti prima di lei — limite in *turni*, non in tempo;
- rara ammessa con tutte le altre lane piene e servita dopo ≤ 16 richieste con 16 lane rumorose (1000 round);
- evento di controllo ammesso su lane dati piena e ordinato **dopo** i dati della lane (barriera);
- wake-up: protocollo produttore con semantica `Notify::notify_one` (permesso memorizzato) e finestra di race esplicita fra "decido di attendere" e "mi registro": nessun wake-up perso; un test di mutazione verifica che la stessa sequenza **senza** permesso memorizzato viene rilevata come wake-up perso;
- mutazioni manuali del codice (quantum off-by-one, deficit non azzerato) fatte fallire dalla proptest.

Simulatore a eventi discreti (`scada_sched::sim`, `cargo run --release -p scada-sched --example policy_model`) — **modello, non misura** — con I/O infinitamente veloce, servizio = 1 µs + 30 ns/oss. (ordine di grandezza di vs002.md §2/§4), 16 sorgenti × 128 oss. pipeline 32 + 1 rara open-loop ogni 0,5–1 ms:

| politica | batch hv/s (modello) | rara p99 (µs, modello) | richieste davanti alla rara (p99) | picco coda |
|---|---:|---:|---:|---:|
| A FIFO Q64 | 206 328 | 388 | 79 | 64 |
| A FIFO Q16 | 206 328 | 156 | 31 | 16 |
| B cap 1, Q64 | 206 328 | 83 | 16 | 17 |
| B cap 2 / 4 / 8, Q64 | 206 328 | 161 / 316 / 388 | 32 / 64 / 79 | 33 / 64 / 64 |
| C RR, lane 1 / 4 / 8 | 206 328 | 83 / 83 / 83 | 16 | 17 / 65 / 129 |
| C RR quantum 4, lane 4 | 206 328 | 315 | 64 | 65 |
| D DRR (quantum 128), lane 4 | 206 328 | 78 | 15 | 65 |

Lettura: (1) nel modello il throughput non cambia perché il sequencer è sempre occupato (I/O infinito); nella realtà B con `c = 1` rischia di lasciare il sequencer senza lavoro mentre le sessioni leggono il frame successivo — **va misurato**. (2) C ottiene la stessa attesa di B `c = 1` *qualunque* sia la profondità delle lane, quindi può tenere più buffering per il throughput senza penalizzare la rara: è l'argomento per C, da confermare. (3) Quantum > 1 annulla il beneficio: quantum fisso a 1. (4) D guadagna solo l'1–2 % sul modello (la rara ha già costo minimo): **DRR non giustificato** finché una misura non mostra squilibrio fra batch corti e lunghi. (5) Il modello prevede p99 ~0,4 ms per A, contro ~6–8 ms misurati: la maggior parte del tail reale è fuori dalla coda del sequencer (§9).

## 4. Semantica proposta (variante C; B ne è il sottoinsieme senza `FairQueue`)

### 4.1 Ammissione e possesso

```
 sessione                         kernel (possiede)                     sequencer
 ────────                         ─────────────────                     ─────────
 frame letto → decode/convert
 reserve() slot ACK (per conn.)
 acquire_owned(permit sorgente) ─┐  (cancel-safe: drop = rinuncia)
 acquire_owned(permit globale)  ─┤
 lock; push(lane, req+permits)  ─┴→ [ lane s ] ── RR ──→ pop ── apply ── reply ── drop(permits)
 notify Condvar se vuota→non vuota
```

- **Punto di possesso:** la richiesta è del kernel quando `push` è riuscito sotto lock. Prima è della sessione: se la sessione viene cancellata (takeover, chiusura, shutdown) mentre attende un permit, il batch decodificato è scartato **senza esito** e lo slot ACK già prenotato (`Pending::Wait`) vede il `oneshot` chiuso → il writer chiude la connessione senza inventare ACK (comportamento VS-002, I4/I5). Il driver riconcilia via `Welcome.last_accepted_source_sequence`.
- **Ordine di acquisizione fisso:** slot ACK → permit sorgente → permit globale → lock. Nessun ciclo di attesa → nessun deadlock. I permit (`OwnedSemaphorePermit`) viaggiano **dentro** la richiesta e sono rilasciati dal sequencer dopo `apply` (o al drop della richiesta in shutdown): nessun leak possibile senza leak della richiesta stessa.
- **Limiti:** per sorgente `per_source_pending_limit` (`c`), globale `input_queue_capacity` (`Q`, stessa chiave e stesso significato di VS-002: richieste possedute in attesa di `apply`). Con un permit sorgente prima del globale, nel semaforo globale c'è al più **un** waiter per sessione, quindi la rara attende al più `max_driver_connections − 1` ammissioni globali più `(lane attive − 1)` servizi RR.
- Rapporto con gli ACK: invariato. `max_inflight_per_connection` resta il limite degli ACK pendenti per connessione; `c ≤ max_inflight_per_connection` è richiesto (altrimenti `c` non avrebbe effetto) e validato.

Perché semafori Tokio e non `Popped::freed` + `Notify`: `Semaphore::acquire_owned` è cancel-safe e FIFO, i permit tornano col drop, e la gestione di due sessioni della stessa sorgente in takeover (stesso semaforo) è corretta senza codice di wake-up proprio. Il flag `freed` del modello resta come alternativa documentata (e testata nel modello) se i semafori risultassero più costosi.

### 4.2 Ordine intra-sorgente

Una lane per **sorgente** (non per link): tutti i batch di una generazione entrano nella lane nell'ordine del socket (un solo reader per sessione) e la lane è FIFO. Esiti validi/invalidi restano nell'ordine della coda ACK della sessione (invariata). Nessun ACK prima di `apply` (writer invariato).

### 4.3 Interleaving fra sorgenti

Nuova politica deliberata: round-robin fra lane non vuote nell'ordine in cui sono diventate attive, una richiesta per turno. `operation_sequence` segue il commit effettivo. L'ordine globale cambia rispetto alla FIFO e fra esecuzioni (già oggi non riproducibile fra processi, `architecture.md`): i test confrontano **esiti per sorgente e stato normalizzato**, e l'identità del digest solo per replay dello **stesso** stream catturato (brief §7 "attenzione importante").

### 4.4 Connect / takeover

- `Connect` entra nella lane della propria sorgente come richiesta di **controllo**: non consuma permit sorgente/globale (ha un contatore proprio e limitato, §4.6), quindi un nuovo `Hello` è ammesso anche con lane, coda globale o semafori saturi. Viene servito dopo i ≤ `c` batch già nella lane (del vecchio link) e dopo al più `(lane attive − 1)` turni.
- Batch del vecchio link **già nella lane prima** del `Connect`: applicati con la vecchia generazione (ancora attiva), ACK al vecchio link se vivo.
- Batch del vecchio link ammessi **dopo** il `Connect` (race fra push e segnale `superseded`): servizi dopo il `Connect`, il core li rifiuta `LINK_SUPERSEDED` (esito tipizzato, nessun falso ACK).
- Dopo il `Welcome` il sequencer segnala `superseded` (D3, invariato); il reader del vecchio link interrompe anche l'attesa dei permit (select) — nessuna nuova ammissione da un link superato oltre la race sopra.
- Riconciliazione: `Welcome.last_accepted_source_sequence` come in VS-002.

### 4.5 Disconnect (barriera)

`Disconnect` entra nella lane della sorgente come controllo, **dopo** tutti i batch che la sessione ha ammesso (la sessione lo invia dopo la fine di reader e writer, come oggi): la FIFO della lane è la barriera per link/generazione. Una `Disconnect` stale (vecchia generazione) resta `Ignored(StaleLinkGeneration)` nel core e non degrada il nuovo link.

### 4.6 Control plane

- `Connect`/`Disconnect`: nella lane della sorgente (§4.4–4.5), non soggetti ai permit dati. Ogni sessione ha al più **una** richiesta di controllo in coda alla volta (il `Connect` riceve risposta prima della fase dati; la `Disconnect` è l'ultimo atto) → totale controllo ≤ `max_driver_connections`. Per lane si preallocano `c + 2` posti; una lane può crescere fino a `c + max_driver_connections` solo in un flood di `Hello` sulla stessa sorgente (crescita limitata, contata nel budget al caso peggiore). **Punto aperto O1**: preallocare tutto (`c + max_conn` per lane, memoria × sorgenti) o crescita limitata; si decide misurando RSS.
- `Introspect`: lane di controllo dedicata (indice `sorgenti`), servita in RR come una sorgente; ≤ 4 richieste (4 sessioni stop-and-wait). Nessuna corsia a priorità assoluta. L'introspezione ferma il writer per la durata dello snapshot: interferenza misurata nella campagna (workload diagnostico facoltativo).

### 4.7 Shutdown e guasti

| Evento | Effetto |
|---|---|
| Ultimo `Server` (produttori) rilasciato | flag `closed` sotto lock + `Condvar::notify_all`; il sequencer svuota la coda (applica tutto ciò che è posseduto, come oggi la `mpsc` fino a esaurimento) e termina |
| Panic del sequencer | guardia di drop: `closed = true`, `Semaphore::close()` su tutti i semafori → `acquire` in attesa ritornano errore → sessioni chiuse; richieste in coda droppate → `oneshot` chiusi → writer chiude senza ACK; host termina (`sequencer thread stopped unexpectedly`, invariato) |
| Reader cancellato in attesa di permit | `Acquire` droppato: nessun permit consumato, batch non posseduto (§4.1) |
| Writer bloccato su peer non lettore | invariato (D2, `ack_write_timeout_ms`); il reader si ferma sullo slot ACK, non su permit kernel |
| Takeover incrociati | ogni `Connect` è controllo nella propria lane; l'ultimo applicato vince (generazione più alta), gli altri link ricevono `superseded` |

### 4.8 Concorrenza e cancel-safety

- Niente `select!` su `mpsc::send()` che possa abbandonare il valore: l'attesa è solo su `acquire_owned` (cancel-safe per documentazione Tokio); il `push` è sincrono sotto lock e non può fallire per spazio (i permit lo garantiscono; il controllo di capacità del modello resta come difesa e restituisce il valore).
- Lato sequencer: `std::sync::Mutex<FairQueue> + Condvar`; il produttore notifica solo nella transizione vuota→non vuota **sotto lock** (nessun wake-up perso: il sequencer ricontrolla `is_empty`/`closed` sotto lo stesso lock prima di attendere). Sezioni critiche O(1).
- Rischio da misurare: contesa del mutex fra worker I/O e sequencer con `multi_thread(N)`; alternativa se dannosa: mantenere la `mpsc` come trasporto e la `FairQueue` interna al solo sequencer (che dreni la `mpsc` in lane) — **ma** questo reintroduce buffering FIFO davanti alle lane (ADR 0005) e va scartato se peggiora la rara.

## 5. Configurazione proposta (minima, additiva)

```toml
[ipc]
ingress_policy = "fifo"            # default VS-002; "source_fair" = variante C; ("source_capped" = B solo se vince)
per_source_pending_limit = 4       # solo con policy != fifo; 1..=max_inflight_per_connection
```

Nessuna chiave per quantum (fisso 1) né per DRR finché una misura non li giustifica; nessuna `control_queue_capacity` (derivata da `max_driver_connections`). Valori invalidi rifiutati con errore esplicito (test). Configurazioni VS-002 invariate → comportamento identico (I9).

## 6. Budget memoria (`budget.rs`)

Variante C, aggiunte al bound `u128` (tutti prodotti saturanti di limiti configurati):

- lane: `sorgenti_config + 1` × (`c` + 2) × `size_of::<Entry<Request>>()` preallocati, + `max_driver_connections` × `size_of::<Entry<Request>>()` × 2 (crescita limitata del controllo, fattore 2 per il raddoppio di `VecDeque`);
- anello ready: (`sorgenti_config + 1`) × 4 B; flag/contatori per lane;
- semafori: (`sorgenti_config`) × stato del semaforo (stima fissa documentata come tale);
- richieste possedute con il loro `Vec<Observation>`: min(`Q`, `sorgenti·c`) + 1 (invece di `Q` + 1);
- il termine P1 di VS-002 (decode transitorio per worker) resta invariato e va **ricontrollato** sullo SHA finale: l'ingresso fair non cambia la decodifica, ma il test di regressione P1 deve passare anche con `ingress_policy = "source_fair"`.

## 7. Piano di prova (brief §7 → test)

| Brief | Test previsto |
|---|---|
| 1 equivalenza core | cattura sotto `source_fair` → replay su kernel nuovo = digest live; core invariato |
| 2 ordine per sorgente | 16 sorgenti × N, `ct` e `mt2`, sequenze/ACK 1..N, con dedup/gap/`FullState` |
| 3 no perdita/duplicato | contatori `applied+duplicate+rejected` = batch ammessi; ogni ACK nell'ordine |
| 4 fair progress | rumorosa sempre pronta + rara: servizio entro ≤ `lane attive` turni (contatore di turni nel sequencer, test con throttle) |
| 5 wake-up | modello (fatto) + stress: produttori su transizioni vuota/non vuota, chiusure |
| 6 cancellazione | reader distrutto in attesa di permit; takeover concorrente; `available_permits` torna al massimo a riposo (no leak) |
| 7 takeover/barriera | vecchio link con lane piena + nuovo `Hello`: `Welcome` entro limite; stale `Disconnect` non degrada; `ct` e `mt2` |
| 8 congestione | max conn/inflight/per-source/globale; payload avversario P1 sotto `source_fair` |
| 9 introspezione | coerenza contatori; introspezione non lettrice non blocca il writer |
| 10 shutdown | SIGTERM con sessioni in attesa di permit; nessun task sospeso |
| 11 risorse | `memory_budget_bytes` con molte sorgenti × `c`; overflow `u128` |
| 12 fuzz/proptest | modello (fatto); fuzz VS-002 invariati |

## 8. Criteri di decisione (prima della campagna)

Adottare la variante **minima** che, sulla baseline VS-002 finale, stesso host e stessa metodologia (7–10 run randomizzati):

1. riduce il p99 della rara in modo ripetibile (permutation test p < 0,05 e oltre la dispersione), obiettivo sperimentale ~2× a throughput hv comparabile;
2. con regressione di throughput hv ≤ ~10 % in almeno 16hv/Q64 `mt2`, senza regressioni importanti nelle altre celle (underload 1 client, batch piccoli, `ct`);
3. senza aumento sproporzionato di CPU, RSS, allocazioni per batch, risvegli;
4. con tutti i test §7 verdi.

Ordine di preferenza a parità di beneficio: A (nessun cambiamento) < B (solo semaforo) < C < D. Se nessuna variante soddisfa 1–2, **si mantiene la FIFO** e si documenta perché.

## 9. Profilo preliminare su `fd5e992` (orientativo, NON baseline)

Dettagli e tabella in `docs/benchmarks/vs003.md` §1. Sintesi: con `mt2`, 16 hv, Q64 l'attesa in coda del sequencer è ~37 % del p99 end-to-end della rara (2,9 su 7,8 ms); il resto è scheduling dei writer sui worker saturi e contesa CPU con i client. Con `ct` la coda pesa ancora meno (thread runtime saturo). Il limite superiore stimato del guadagno della variante C sul solo termine di coda è ~1,5× su `mt2`, quindi l'obiettivo ~2× **non è atteso** dalla sola politica di servizio: la campagna dovrà dire se la diversa ammissione (meno batch pendenti per sorgente) riduce anche il termine `reply_wait`. Se no, la conclusione onesta potrà essere B (semplice) o FIFO.

## 11. Decisione (dopo la campagna, `docs/benchmarks/vs003.md` §4)

**Fatti misurati** (VM 4 vCPU con client co-residenti, 7 run randomizzati per cella, 532 run in tutto, 0 falliti; Δ rispetto alla FIFO, `**` = ripetibile):

| scenario | variante | rara p99 | throughput hv |
|---|---|---:|---:|
| Server `mt2`, 16 hv, Q64 (cella del target) | C4 (`source_fair`, 4/sorgente) | +6 % (n.s.) | −10 % (n.s.); −11 % ** in 8×128 |
| | C1 | **−60 %** | **−47 %** |
| | B1 (`source_capped`, 1) | **−56 %** | **−47 %** |
| | B4 | +5 % (n.s.) | −1 % (n.s.) |
| | FIFO Q16 (VS-002, riferimento) | −46 % | −43 % |
| `mt2`, 16 hv × 1 oss. | C4 | +10 % (n.s.) | **−39 %** |
| Micro `ct`, 16 hv, Q16 | C4 | **−75 %** (15,7 → 4,0 ms) | **−21 %** |
| | B1 / C1 | **−73 %** / **−62 %** | **−14 %** / **−20 %** |
| | B4 | +8 % (n.s.) | +1 % (n.s.) |
| `ct`, 8 hv, Q16 | C4 / B4 | +13 % / +5 % (n.s.) | +7 % / +11 % (n.s.) |
| | B1 / C1 | **−46 %** / **−53 %** | **−53 %** / **−55 %** |
| underload 1×1 stop-and-wait (`mt2`, `ct`) | C4 | p99 +10 % ** su `mt2` (205 → 227 µs), ~ su `ct` | ~ |
| fifo del nuovo binario vs VS-002 `d9502c1` | A | nessuna differenza ripetibile | nessuna |

**Perché il modello (§3.1) sbagliava:** con `--diag` (c2) C4 riduce davvero l'attesa in coda della rara (p50 434 → 131 µs) ma il suo p99 end-to-end non cambia: su questo host il tail è dominato da `reply_wait` (risposta pronta, task writer non ancora eseguito: 2,8–3,1 ms al p99) e dal tempo fuori dal kernel, cioè dalla saturazione CPU dei 2 worker I/O e dei client sugli stessi 4 vCPU. Le varianti con 1 batch per sorgente (B1/C1) migliorano il p99 perché **dimezzano il lavoro ammesso**, non per l'equità: stanno sulla stessa frontiera di Pareto di una coda globale corta (Q16).

**Decisione:**

1. **Nessuna variante raggiunge il target sperimentale** del brief (p99 rara ~2× con throughput hv ≥ −10 % in 16hv/Q64 server). Per §8 e brief §9 **la FIFO resta il default** e i profili Micro/Server non cambiano.
2. **`source_fair` (C) resta come opzione esplicita sperimentale**, perché (a) ha l'unica garanzia logica nuova provata end-to-end (attesa della rara ≤ un turno per sorgente attiva: 36 ms contro 174 ms con sequencer rallentato a 2 ms/richiesta, `vs003.rs`), (b) ammette un `Hello` anche con le code dati sature, (c) nel caso Micro saturo (`ct`, Q16, 16 hv) dà −75 % di p99 rara a −21 % di throughput, (d) è neutra in underload su `ct`, in RSS e senza perdita di correttezza. **Costo dichiarato:** −10…−39 % di throughput su `multi_thread` senza beneficio di tail; non consigliata per il profilo Server.
3. **B (`source_capped`) non è tenuta**: dove migliora (cap 1) equivale a ridurre la coda globale (già configurabile con `input_queue_capacity`), con cap 4 non ha effetto. Codice riproducibile dal commit `b21dc31`.
4. **D (DRR) non implementata**: il modello non mostra squilibrio utile (−1/2 % di attesa) e la misura mostra che l'ordine di servizio non è il termine dominante.
5. Il default resta modificabile solo con una nuova campagna che mostri il beneficio su hardware con core dedicati (vedi lavoro futuro in `vs003.md` §6).

## 12. Fonti

- Tokio `Semaphore::acquire_owned`, `mpsc::Sender::reserve` (cancel safety): https://docs.rs/tokio/latest/tokio/sync/
- M. Shreedhar, G. Varghese, *Efficient Fair Queuing Using Deficit Round-Robin*, IEEE/ACM ToN 4(3), 1996.
- `docs/benchmarks/vs002.md` §§2, 4, 5, 7; ADR 0004, 0005, 0008, 0009.
