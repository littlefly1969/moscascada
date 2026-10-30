# ADR 0002 — Stato del kernel, transizioni deterministiche e digest

- **Stato:** accettato per VS-001
- **Data:** 2026-10-30

## Contesto

Brief §1.3, §1.6, §1.7, §4, §5: state machine pura e sincrona con un solo proprietario delle scritture; atomicità per `ObservationBatch`; determinismo dato un ordine di input già sequenziato; digest canonico versionato; nessuna `HashMap` che influenzi stato o digest. Riferimento teorico: Schneider 1990 (<https://doi.org/10.1145/98163.98167>): una replica è una macchina a stati deterministica; l'accordo sull'ordine è un problema distinto, che qui resta fuori dal core.

## Decisioni

### 1. API del core (`scada-core`)

```rust
impl Kernel {
    pub fn new(config: ValidatedConfig) -> Kernel;
    pub fn apply(&mut self, input: SequencedInput) -> Result<TransitionOutcome, ApplyError>;
    pub fn logical_snapshot(&self) -> LogicalSnapshot;
    pub fn state_digest(&self) -> [u8; 32];
}
```

`apply` non fa I/O, non legge orologi, non crea thread, non usa casualità. Dipendenze del crate: solo `blake3` (feature `pure`, nessun codice C/asm compilato). `#![forbid(unsafe_code)]`. Il crate non conosce Tokio, socket, Protobuf, TOML.

### 2. Due ordini distinti

- **Ordine di ammissione (ingress admission order):** l'ordine in cui il sequencer del runtime estrae le richieste dalla coda limitata. Non è parte dello stato logico e non è persistito (non esiste journal in VS-001).
- **Ordine delle transizioni applicate (applied transition order):** `operation_sequence`, assegnato dal sequencer come `last_operation_sequence + 1` e verificato dal core. Avanza **solo** se `apply` restituisce `Ok(TransitionOutcome::Applied { .. })`. Rifiuti (`Err`), duplicati (`Ok(Duplicate)`) e disconnessioni obsolete (`Ok(Ignored)`) non consumano un numero: il sequencer ripropone lo stesso numero all'input successivo.
- Un input con `operation_sequence != last + 1` viene rifiutato con `OperationSequenceMismatch` e stato invariato (test 10). Due esecuzioni sono equivalenti solo se la sequenza applicata coincide: il core **non** afferma che permutazioni diverse diano lo stesso stato.

### 3. Operazioni normalizzate

```text
SourceConnected    { source_id, link_generation, source_epoch }
SourceDisconnected { source_id, link_generation }
ObservationBatch   { source_id, link_generation, mode: FullState|Delta, source_sequence, observations[] }
```

`link_generation` è assegnato dal sequencer (contatore globale crescente) ed è parte dell'input: la replica dello stesso stream produce lo stesso stato. `source_epoch` è un `u64` opaco ≠ 0 scelto dal produttore (nuovo a ogni riavvio del produttore).

### 4. Modello di stato

`S = (config_fingerprint, last_operation_sequence, sources[], tags[])`, con:

- metadati immutabili (`ValidatedConfig`, condiviso via `Arc`): tag ordinati per `TagId`, sorgenti ordinate per `SourceId`, per ogni sorgente il binding `channel_id → indice tag` in un `Vec` ordinato (ricerca binaria). Nessuna `HashMap` nel core.
- per sorgente: `link` (`NeverConnected | Connected(gen) | Disconnected(gen)`), `epoch: Option<u64>`, `last_accepted: Option<u64>`, ultimo batch accettato in forma canonica (modalità, sequenza, coppie `(indice tag, valore)` ordinate) per la deduplicazione, `continuity` dell'ultimo batch.
- per tag (celle runtime, `Vec` indicizzato come i metadati): `value: Option<Bool|I64>`, `quality ∈ {Unknown, Good, Uncertain(SourceUnavailable)}`, `revision: u64`.

**Deduplicazione:** invece del solo hash si conserva l'ultimo batch accettato in forma canonica (≤ `max_observations_per_batch` voci, buffer preallocato per sorgente). Il confronto è esatto (nessun rischio di collisione) e senza hash sul percorso caldo; il brief chiede "almeno l'ultimo hash", questo è un superinsieme. Nessuna storia oltre l'ultimo batch.

**Memoria:** tutte le strutture sono dimensionate alla costruzione da configurazione (numero tag, sorgenti, `max_observations_per_batch`); `apply` non fa crescere alcun contenitore oltre la capacità iniziale. L'input `ObservationBatch` porta un `Vec` allocato dal runtime: è limitato in lunghezza e liberato nel core.

### 5. Regole di transizione (sintesi; dettaglio in `docs/contract.md`)

1. `operation_sequence == last + 1` altrimenti `OperationSequenceMismatch`; `last == u64::MAX` ⇒ `OperationSequenceExhausted`.
2. `SourceConnected`: sorgente configurata, `epoch ≠ 0`, `link_generation` > ultima generazione vista per la sorgente. Se un link precedente era attivo è implicitamente chiuso (takeover). I tag con valore della sorgente passano a `Uncertain(SourceUnavailable)` (la riconnessione non rende `Good`). Epoch diversa ⇒ azzeramento dello stato di deduplicazione (serve un nuovo `FullState`); epoch uguale ⇒ si mantiene `last_accepted` (ripresa senza perdita).
3. `SourceDisconnected`: se `link_generation` è la generazione attiva ⇒ link disconnesso, tag con valore ⇒ `Uncertain(SourceUnavailable)` con valore preservato; se è una generazione più vecchia o già disconnessa ⇒ `Ignored(StaleLinkGeneration)` senza effetti; se è una generazione mai vista ⇒ `UnknownLinkGeneration`.
4. `ObservationBatch`: validazione completa prima di qualsiasi mutazione (sorgente, link attivo, dimensione, canali noti, nessun duplicato, tipi, completezza del `FullState`, regole di sequenza, overflow di revisione). Solo dopo, applicazione in ordine di `TagId`.
5. Sequenze per sorgente: `FullState` senza baseline nell'epoca ⇒ accettato con qualunque `source_sequence ≥ 1`, `continuity = BaselineEstablished` (continuità storica non dimostrata). Con baseline: `seq == last+1` ⇒ `Continuous`; `seq > last+1` ⇒ solo `FullState` accettato come `Resynchronized`, `Delta` ⇒ `SequenceGap`; `seq == last` ⇒ `Duplicate` se il payload canonico coincide, altrimenti `SequenceConflict`; `seq < last` ⇒ `StaleSequence`; `last == u64::MAX` e serve `last+1` ⇒ `SequenceOverflow`; `Delta` senza baseline ⇒ `ResyncRequired`.

### 6. Revisioni e qualità

La revisione di un tag aumenta di 1 solo se cambia la coppia `(value, quality)`. Un valore identico riosservato con qualità già `Good` non cambia la revisione, ma l'operazione è comunque applicata (avanza `operation_sequence` e lo stato di sequenza della sorgente). Overflow della revisione ⇒ `RevisionOverflow` in validazione, nessuna mutazione. Un tag mai osservato resta `Unknown` anche alla disconnessione (non c'è un valore di cui dubitare). `fault=true` ricevuto correttamente è un valore `Good`.

### 7. Digest canonico v1

BLAKE3 (<https://docs.rs/blake3/latest/blake3/>, output 32 byte) su una codifica canonica versionata, alimentata in modo incrementale (`Hasher::update`, nessuna allocazione):

```text
"scada.kernel.state" 0x00 | u16 BE versione=1 | config_fingerprint[32] | u64 BE last_operation_sequence
u32 BE n_sources ; per sorgente in ordine di SourceId:
  u32 source_id | link: u8 (0 never,1 connected,2 disconnected) + u64 generation
  | epoch: u8 presente + u64 | last_accepted: u8 + u64 | continuity: u8
  | ultimo batch: u8 presente + u8 mode + u64 seq + u32 n + n × (u32 tag_id, valore marcato)
u32 BE n_tags ; per tag in ordine di TagId:
  u32 tag_id | u8 tipo | valore marcato (u8 0=none,1=bool+u8,2=i64+i64 BE) | u8 qualità | u8 motivo | u64 revisione
```

Esclusi: `kernel_instance_id`, handle, contatori metrici, wall-clock, ordine di ammissione. Il `config_fingerprint` è BLAKE3 di una codifica canonica analoga della configurazione validata (versione 1, entità ordinate per ID). Il digest è calcolato su richiesta, mai in `apply`.

## Alternative considerate

- **Hash dell'ultimo batch (32 byte) invece della copia canonica:** meno memoria, ma richiede BLAKE3 su ogni batch nel percorso caldo e ammette collisioni teoriche. Scartato a favore del confronto esatto (memoria comunque limitata: `max_observations_per_batch × 16 byte` per sorgente).
- **SHA-256 per il digest:** equivalente funzionalmente; BLAKE3 è indicato dal brief ed è implementabile in Rust puro (feature `pure`) senza toolchain C per ARM64.
- **Rifiuti che consumano `operation_sequence`:** utile per un futuro journal di ammissione; in VS-001 manterrebbe nel log ingressi senza effetto. Rimandato: l'ordine di ammissione è già distinto concettualmente e potrà diventare un contatore separato.
- **Rifiuto di una seconda connessione per la stessa sorgente invece del takeover:** più semplice, ma un driver riavviato rapidamente verrebbe bloccato finché il kernel non rileva l'EOF del link precedente. Con il takeover il link vecchio diventa obsoleto e i suoi batch e la sua disconnessione non hanno effetto (test 8, 16).
