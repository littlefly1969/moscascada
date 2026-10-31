# ADR 0007 — Ricerca dei canali e ordinamento nel core (C4)

- **Stato:** accettato (VS-002)
- **Data:** 2026-10-31
- **Ambito:** `scada-core::config::SourceMeta::tag_index`, `Kernel::batch`.

## Contesto

`Kernel::apply` per un batch di *n* osservazioni fa, per ciascuna, una ricerca `channel_id → indice tag` (VS-001: ricerca binaria su `Vec<(ChannelId, u32)>` ordinato), un controllo di tipo e un push nello staging; poi ordina lo staging per indice tag, controlla i duplicati, applica. Il brief (§5 C4) chiede di provare: fast path per batch già in ordine canonico, indicizzazione densa per sorgenti compatibili senza peggiorare il footprint di quelle sparse, eliminazione dell'`Arc::clone` della configurazione per batch; senza toccare `TagCell` né la politica di deduplicazione.

## Metodo

- Microbenchmark Criterion `apps/scada-bench/benches/micro.rs` (`apply/{sorted,reversed,shuffled}/{1,32,128}`, sorgente con 128 canali, ogni osservazione cambia valore; input costruiti nel setup non cronometrato).
- Prima prova: confronto fra baseline Criterion salvate su commit successivi (`--save-baseline` / `--baseline`). **Scartata come prova**: sulla VM (4 vCPU KVM) due run consecutive dello stesso codice differiscono fino al ±20 %; il confidence interval di Criterion copre solo il rumore *dentro* una run. Log in `docs/benchmarks/results/vs002/micro/c4-*-vs-*.txt` (incluso un caso in cui codice identico, compilato in due crate diversi, risultava diverso del 50 %).
- Prova adottata: `scripts/micro_ab.py`, quattro binari di benchmark (c2 = VS-001 core, `arc` = solo split dei borrow, `arc_dense` = + indice denso, `arc_dense_sorted` = + pre-check `is_sorted`) eseguiti 7 volte ciascuno in ordine casuale; un campione = mediana Criterion di una run. Test di permutazione esatto sulle medie (`scripts/bench_analyze.py`). Dati grezzi: `docs/benchmarks/results/vs002/micro/c4-interleaved.txt`.

## Risultati (mediane di 7 run, Δ rispetto a c2; ** = p < 0,05 e |Δ| oltre la semi-dispersione della baseline)

| caso | split borrow (no `Arc::clone`) | + indice denso | + `is_sorted` (vs indice denso) |
|---|---:|---:|---:|
| sorted/1 | ~−11 % (p 0,10) | **−32 %** | ~+9 % (p 0,77) |
| sorted/32 | **−12 %** | **−28 %** | ~+2 % (p 0,90) |
| reversed/32 | ~+3 % (p 0,93) | ~−16 % (p 0,045, sotto la dispersione) | ~−11 % (p 0,19) |
| shuffled/32 | ~−9 % (p 0,19) | **−20 %** | ~+3 % (p 0,53) |
| sorted/128 | ~−11 % (p 0,03, sotto la dispersione) | **−28 %** | ~−14 % (p 0,02, sotto la dispersione) |
| reversed/128 | ~−18 % (p 0,01, sotto la dispersione) | **−24 %** | ~−10 % (p 0,04, sotto la dispersione) |
| shuffled/128 | ~−8 % (p 0,29) | ~−11 % (p 0,002, sotto la dispersione) | ~−1 % (p 0,78) |

Il pre-check `is_sorted` non può aiutare i casi `reversed`; il fatto che mostri −10 % proprio lì misura il rumore residuo (~10 %) anche con il metodo interlacciato.

## Decisione

1. **Adottato: indice denso** per sorgenti con numerazione compatta (`span = max − min + 1 ≤ 2 × numero canali`): tabella `Vec<u32>` `channel − base → indice tag` (buchi = `u32::MAX`), ≤ 8 byte per canale configurato, costruita una volta in `ValidatedConfig::validate`. Le sorgenti sparse non pagano nulla (tabella vuota, ricerca binaria come in VS-001). Guadagno ripetibile in 5 casi su 7, mai una regressione.
2. **Non adottato: `is_sorted` prima dell'ordinamento.** Nessun effetto oltre il rumore: l'ordinamento instabile della libreria standard riconosce già in tempo lineare input ordinati o invertiti.
3. **Non adottato: eliminazione dell'`Arc::clone`** per batch. Effetto non ripetibile (p > 0,05 in 4 casi su 7); il brief chiede di eliminarlo "soltanto se utile". Il codice resta quello di VS-001.
4. La validazione dei duplicati resta completa in ogni caso (controllo su finestre adiacenti dopo l'ordinamento). `TagCell` e la deduplicazione sono invariati.

## Equivalenza

- `crates/scada-core/tests/reference/vs001_kernel.rs`: copia congelata del `Kernel` VS-001 (commit b17413f), usata come specifica eseguibile.
- `tests/equivalence.rs`: 200 stream seminati × 400 passi + 256 casi proptest su configurazione con sorgente densa, sparsa e a canale singolo, osservazioni ordinate/invertite/mescolate, duplicati adiacenti e non, canali sconosciuti, tipi errati, batch vuoti e sovradimensionati, gap, stale, duplicati, takeover: esito e snapshot logico identici a ogni passo. Più un controllo esaustivo che `tag_index` coincida con la ricerca binaria per ogni `channel_id` in 0..13 100 e `u32::MAX`.
- `crates/scada-runtime/tests/vs002.rs::captured_streams_replay_to_the_live_digest_in_every_mode`: gli stream effettivamente ammessi da kernel reali sono riapplicati sia al kernel ottimizzato sia a quello VS-001, con esiti e snapshot identici.

## Footprint

Per la configurazione di benchmark (16 sorgenti × 128 canali consecutivi + 1 sorgente da 1 canale) la tabella densa aggiunge 16 × 128 × 4 = 8 KiB. Il bound è incluso in `ValidatedConfig::heap_bytes()` e quindi nel budget di memoria (ADR 0009).
