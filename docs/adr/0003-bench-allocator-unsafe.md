# ADR 0003 — Eccezione `unsafe` isolata: allocatore di conteggio nel benchmark

- **Stato:** accettato
- **Data:** 2026-10-30

## Contesto

Il brief §1.8 vieta `unsafe` applicativo salvo eccezioni isolate, motivate e approvate via ADR. Il brief §8 chiede "allocazioni per operazione quando misurabili". In Rust stable l'unico modo di contare le allocazioni del processo è installare un `#[global_allocator]`, che richiede `unsafe impl GlobalAlloc`.

## Decisione

- L'eccezione vale **solo** per `apps/scada-bench/src/alloc.rs` (circa 30 righe), che inoltra ogni chiamata a `std::alloc::System` e incrementa contatori atomici.
- `scada-bench` non è un componente di esercizio: non viene distribuito né caricato da kernel o driver.
- Il crate dichiara `unsafe_code = "deny"` e il solo modulo `alloc` lo sblocca con `#![allow(unsafe_code)]`; tutti gli altri crate (`scada-core`, `scada-protocol`, `scada-runtime`, `scada-sim-driver`) hanno `#![forbid(unsafe_code)]` o la lint di workspace `unsafe_code = "forbid"`.
- I test di integrazione inviano `SIGTERM` tramite il comando `kill` proprio per evitare `libc::kill` e quindi `unsafe`.

## Alternative

- Crate esterni (`stats_alloc`, `dhat`): spostano lo stesso `unsafe` in una dipendenza non revisionata qui; nessun guadagno di sicurezza.
- Nessuna misura delle allocazioni: il requisito §8 resterebbe insoddisfatto senza motivo tecnico.

## Verifica

`grep -rn "unsafe" crates apps --include=*.rs` deve trovare occorrenze solo in `apps/scada-bench/src/alloc.rs` (e commenti). Lo script `scripts/check.sh` lo controlla.
