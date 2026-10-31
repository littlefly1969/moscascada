# ADR 0005 — Drain del sequencer: `blocking_recv` vs `blocking_recv_many` (C1)

- **Stato:** **misurato e non adottato** (VS-002). Il codice è stato rimosso dopo la misura (commit 4eb8968); la variante resta riproducibile dal commit 272cd2a.
- **Data:** 2026-10-31
- **Ambito:** `scada-runtime::sequencer::run`.

## Contesto

Il thread sequencer in VS-001 preleva una richiesta per risveglio (`mpsc::Receiver::blocking_recv`). Il brief (§5 C1, §11) chiede di confrontare un drain limitato (`blocking_recv_many(N)`, o `try_recv` ripetuto) con N = 1/4/16/64, senza attese per riempire un gruppo, senza fusioni e senza riordino.

Documentazione: [`Receiver::blocking_recv_many`](https://docs.rs/tokio/latest/tokio/sync/mpsc/struct.Receiver.html#method.blocking_recv_many) preleva fino a `limit` messaggi già presenti, attendendo solo se la coda è vuota. `try_recv` in un ciclo dopo un `blocking_recv` ha lo stesso effetto osservabile con più chiamate; non è stato misurato separatamente perché `recv_many` è la forma con meno operazioni sul canale.

## Implementazione provata

`sequencer_drain_max = N` (1..=1024, default 1). Con N > 1: un `Vec<Request>` preallocato di capacità N, `blocking_recv_many(&mut v, N)`, poi ogni richiesta applicata **una alla volta nell'ordine della coda** (stesso `handle` del percorso N = 1). Un istogramma `drain_size` misurava quante richieste arrivavano per risveglio.

Effetto sul modello di risorse, documentato prima della misura: le richieste prelevate escono dalla coda e liberano posti nel semaforo dell'`mpsc`, quindi il buffering effettivo fra sessioni e core diventa `input_queue_capacity + N`. Il bound di memoria (ADR 0009) e, soprattutto, il tempo di attesa di una sorgente rara crescono di conseguenza: la coda Tokio è FIFO, quindi un batch appena ammesso attende al più ~(posti davanti a lui) × (tempo di servizio).

## Misure

Campagna 3 (`docs/benchmarks/results/vs002/e2e/c3-rare.txt`): sorgente rara 900 (1 canale, un batch ogni 1 ms in stop-and-wait) contro 0/2/8/16 sorgenti ad alto volume (128 oss., pipeline 32); 9 varianti × 5 celle × 7 run randomizzati, 6 s per run. Tabelle complete: `c3-rare-vs-ct.md`, `c3-rare-vs-mt2.md`.

Δ rispetto a N = 1 (`current_thread`), ** = ripetibile (p < 0,05 e oltre la dispersione della baseline):

| cella | N | rara p50 | rara p99 | rara p99.9 | throughput alto volume |
|---|---:|---:|---:|---:|---:|
| 2 hv, coda 64 | 4 / 16 / 64 | −11 % / −7 % / −10 % (n.s.) | −9 % / −2 % / −3 % (n.s.) | +7 % / −7 % / 0 % (n.s.) | +9 % / +9 % / +12 % (n.s.) |
| 8 hv, coda 64 | 4 / 16 / 64 | +10 % / +40 % / +9 % (n.s.) | −1 % / +12 % / −3 % (n.s.) | +12 % / +30 % / +5 % (n.s.) | −5 % / −16 % / −5 % (n.s.) |
| 16 hv, coda 64 | 4 / 16 / 64 | −4 % / +11 % / +1 % (n.s.) | +3 % / +9 % / +7 % (n.s.) | −34 % / −30 % / −32 % (p 0,005–0,07, sotto la dispersione della baseline) | +1 % / −5 % / −1 % (n.s.) |
| 16 hv, coda 16 | 4 / 16 / 64 | +45 % / +17 % / +45 % (n.s.) | −3 % / −2 % / 0 % (n.s.) | −3 % / −2 % / +3 % (n.s.) | +4 % / +2 % / +2 % (n.s.) |

Con `multi_thread(2)`, N = 16 rispetto a N = 1: entro il rumore con coda 64; con coda 16 e 16 sorgenti **peggiora ripetibilmente** la sorgente rara (p50 +14 %, p99 **+31 %**) mentre aumenta il throughput ad alto volume (+14 %): esattamente l'effetto `Q + N` previsto sopra (più buffering, più attesa per chi arriva dopo).

## Decisione

**Non adottato; codice rimosso.** Con il sequencer al 15–45 % di CPU (ADR 0004) il costo per risveglio del thread non è il collo di bottiglia, quindi prelevare più richieste per risveglio non porta un vantaggio ripetibile; dove ha un effetto misurabile, peggiora la latenza critica della sorgente rara, che il brief (§9) vieta di peggiorare senza motivazione esplicita. Si conserva la forma più semplice: `blocking_recv`, una richiesta per risveglio, buffering totale = `input_queue_capacity`.

Da rimisurare se un hardware diverso (es. ARM64 con risvegli più costosi) mostrasse il sequencer come collo di bottiglia; la variante è nel commit 272cd2a.
