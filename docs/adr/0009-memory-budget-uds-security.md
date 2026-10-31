# ADR 0009 — Bound configurativo della memoria e sicurezza del filesystem UDS (E1–E3)

- **Stato:** accettato (VS-002)
- **Data:** 2026-10-31
- **Ambito:** `scada-runtime::budget`, `scada-runtime::server::{check_socket_dir, bind_socket, SocketGuard}`, introspezione, `[runtime] memory_budget_bytes`.

## E1 — bound configurativo delle allocazioni

**Decisione.** All'avvio il kernel calcola in `u128` (aritmetica saturante: nessuna configurazione accettata può causare overflow; i fattori sono `u32`/`u64` quindi il risultato reale resta molto sotto `u128::MAX`) un bound conservativo delle allocazioni applicative:

| termine | composizione |
|---|---|
| per connessione | `max_frame_bytes` (buffer payload riusato, cresce fino al frame più grande) + `read_buffer_bytes` + batch decodificato riusato (`max_observations_per_batch × size_of::<pb::Observation>()`) + batch convertito in attesa di slot/coda + `max_inflight × (voce coda + cella oneshot)` + buffer ACK (`(ack_coalesce_max + 1) × 96 B`) + handshake (512 B) + stima fissa per task Tokio (16 KiB) |
| connessioni | per connessione × `max_driver_connections` |
| coda sequencer | `(input_queue_capacity + sequencer_drain_max + 1) × (size_of::<Request>() + batch convertito massimo)` |
| kernel | `Kernel::storage_bound_bytes` (RTDB: celle tag, stato sorgenti, staging) + `ValidatedConfig::heap_bytes` (metadati, indici, tabelle dense) |
| introspezione | 4 sessioni × risposta limitata (se il socket è abilitato) |
| diagnostica | istogrammi fissi (se `--diag`) |

Il risultato è stampato da `--check-config` e su stderr all'avvio; se `[runtime] memory_budget_bytes` è impostato e il bound lo supera, l'avvio è rifiutato (exit 3).

**Cosa non è.** Non è un bound dell'RSS del processo: stack dei thread (2 o N+2), frammentazione e metadati dell'allocatore, strutture interne di Tokio, binario e libc, buffer socket del kernel Linux (memoria kernel, governata da `net.core.*mem`) sono esclusi; lo stato per task Tokio è una **stima** dichiarata (16 KiB). La validità del bound è verificata empiricamente dal soak test (RSS stabile e sotto il bound per la parte applicativa; vedi `docs/benchmarks/vs002.md`).

**Allocazione lazy.** I buffer per connessione partono vuoti e crescono solo fino al frame più grande effettivamente ricevuto (mai oltre `max_frame_bytes`, verificato sull'header **prima** del `resize`); la tabella densa esiste solo per le sorgenti compatte; la diagnostica solo con `--diag`.

Alternative scartate: misura dell'RSS a runtime con rifiuto delle connessioni (dipende da allocatore e kernel, non deterministica); allocatore con contatore globale (il brief vieta un allocatore custom senza prova; resta solo nel bench, ADR 0003).

## E2 — directory e socket

Problemi VS-001: la directory del socket era creata con i permessi dell'umask e non verificata; un socket preesistente era rimosso senza controllo del proprietario; alla chiusura il file era rimosso per percorso (anche se nel frattempo un altro processo lo aveva sostituito).

**Decisione.**

1. La directory, se assente, è creata con mode `0750` direttamente (`DirBuilder::mode`, nessuna finestra dipendente dall'umask).
2. La directory (nuova o preesistente) deve essere: non un symlink (`symlink_metadata`), una directory, di proprietà dell'utente effettivo o di root, non scrivibile da gruppo o altri (`mode & 0o022 == 0`). Altrimenti l'avvio è rifiutato. Una directory come `/tmp` (1777) è quindi rifiutata: un altro utente potrebbe pre-creare o sostituire il socket.
3. Un percorso preesistente: se non è un socket → rifiuto; se è un socket su cui qualcuno ascolta → rifiuto (`AddrInUse`); se è un socket stale di **un altro utente** → rifiuto, non viene cancellato; solo un socket stale dello stesso utente viene rimosso.
4. `SocketGuard` registra `(dev, inode)` del socket appena creato e alla chiusura lo rimuove **solo se** il percorso punta ancora a quello stesso inode.
5. `SO_PEERCRED` e l'allowlist UID sono invariati; il socket riceve il mode configurato (default `0660`) e il gruppo opzionale subito dopo il `bind`.

**Limite residuo.** Fra `bind` e `chmod` il socket ha per un istante il mode derivato dall'umask; la directory non scrivibile da altri e `SO_PEERCRED` limitano l'esposizione. Ridurla del tutto richiederebbe `umask(2)` (stato globale del processo, `libc`/`unsafe` o `nix`) o un `bind` in una directory temporanea privata + `rename`; non adottato per complessità/beneficio. Il controllo dei componenti superiori del percorso (oltre alla directory immediata) non è eseguito: è responsabilità del deployment (es. `/run/<servizio>` creato da systemd con `RuntimeDirectory=`).

Test: `tests/socket_security.rs` (directory scrivibile da altri rifiutata, directory symlink rifiutata, directory creata privata, file non-socket e socket vivo rifiutati, socket stale proprio rimosso, socket stale altrui non rimosso — eseguito solo come root, guard che non rimuove un socket sostituito).

## E3 — introspezione e input malformati

- L'introspezione resta opzionale (disabilitata di default), read-only, su socket separato `0600`, al massimo 4 sessioni, risposta limitata dal frame ceiling, fuori dal percorso caldo (il sequencer risponde fra un input e il successivo). VS-002 aggiunge un **idle timeout** di sessione (stesso valore di `handshake_timeout_ms`) e la scadenza di scrittura: una sessione di introspezione non può più restare aperta indefinitamente occupando uno dei 4 posti.
- `reset_diagnostics` (campo additivo) azzera solo gli istogrammi `--diag`; non tocca stato RTDB né contatori.
- `max_frame_bytes` è verificato sull'header prima di qualsiasi allocazione del payload, anche con il buffer di lettura C5 (il `BufReader` ha capacità fissa e non dipende dalla lunghezza dichiarata). Test: `vs002.rs::malformed_input_with_read_buffer`, fuzz `frame_decode`, `batch_validation` (aggiornato per il decoder C3).

## E4

`cargo audit` e `cargo deny` (licenze, advisories, sorgenti): report in `docs/benchmarks/results/vs002/supply-chain/`. Nessun aggiornamento forzato; `Cargo.lock` versionato.
