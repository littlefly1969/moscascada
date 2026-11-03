# Catalogo dei profili prodotto dei driver (VS-007, ADR 0015 D11)

Un **profilo** descrive *una versione di un prodotto* per un trasporto e un protocollo: i blocchi leggibili del dispositivo e i suoi punti (area, indirizzo PDU, tipo raw, scala). Non contiene identità MoScada: `source_id` e `channel_id` appartengono alla configurazione del driver (`config/examples/modbus-tcp.toml`) e del kernel. Aggiungere un prodotto = aggiungere un file; nulla si ricompila, il kernel non cambia.

```
trasporto (tcp: host, porta, timeout)          -> configurazione del dispositivo
  protocollo (modbus: unit id, FC01..FC04)     -> scada-modbus
    profilo prodotto (id, version, blocchi, punti) -> questo catalogo
      canali/tag MoScada (source_id, channel_id -> tag) -> driver + kernel
```

## File

| File | Contenuto |
|---|---|
| `generic-io.v1.toml` | Profilo **generico di prova** (fixture del simulatore). **Non è un prodotto reale**: nessun marchio, modello o mappa ufficiale. |
| `driver-catalog.schema.json` | Schema JSON (documentale) del formato `moscada.driver-catalog/1`. Il validatore normativo è `scada-driver-modbus-tcp --check-config`. |

## Regole del formato `moscada.driver-catalog/1`

- `schema = "moscada.driver-catalog/1"`; chiavi sconosciute rifiutate.
- `[profile]`: `id` (`[a-z][a-z0-9_-]{0,63}`), `version` ≥ 1, `title`, `manufacturer`, `transport = "tcp"`, `protocol = "modbus"`, `source_document` (provenienza della mappa: documento e revisione; obbligatorio), `max_requests_per_scan` (1..=256, default 32). La coppia `(id, version)` è unica nel catalogo.
- `[[blocks]]`: intervalli **dichiarati leggibili** (`area`, `start`, `count`, `max_per_request` ≤ 125 registri / 2000 bit). Il driver non legge mai fuori da un blocco e non unisce blocchi diversi; blocchi sovrapposti della stessa area sono rifiutati.
- `[[points]]`: `name`, `area` (`coil` | `discrete_input` | `holding_register` | `input_register`), `address` (**PDU, zero-based**), `type`:

| `type` | Aree | Valore MoScada | Note |
|---|---|---|---|
| `bit` | coil, discrete_input | `bool` | |
| `register_bit` | registri | `bool` | `bit = 0..=15` (0 = meno significativo) |
| `u16`, `i16` | registri | `i64` | `byte_swap` opzionale |
| `u32`, `i32` | registri (2) | `i64` | `word_order` **obbligatorio** (`high_first` = ABCD, `low_first` = CDAB) |
| `f32` | registri (2) | `i64` fixed-point | `word_order` e `rounding` (`half_even` / `truncate`) obbligatori; NaN/Inf rifiutati |

  Scala: `value = round(raw × scale_numerator / scale_denominator) + offset` (default 1/1/0, `rounding = "exact"` che rifiuta valori non interi). Overflow verificato al caricamento sugli estremi del tipo. `valid_min` / `valid_max` (opzionali): un valore fuori intervallo è un **errore di lettura** (sorgente degradata), mai un dato `Good`. `unit` e `description` sono documentali.

## Indirizzi

Le tabelle dei costruttori usano spesso etichette `0xxxx`/`1xxxx`/`3xxxx`/`4xxxx` (es. «40001»). Il catalogo usa **solo indirizzi PDU zero-based**: «40001» = holding register, `address = 0`. La conversione si fa una volta, a mano, nel file del profilo, e va annotata in `source_document`. Il driver non aggiunge né sottrae offset.

## Aggiungere un prodotto

1. Copiare `generic-io.v1.toml` in `<prodotto>.v<versione>.toml`, impostare `id`, `version`, `manufacturer`, `source_document` con il riferimento esatto della mappa ufficiale.
2. Dichiarare solo i blocchi documentati come leggibili; raggruppare nello stesso blocco i valori che devono essere coerenti fra loro (una scansione non è una fotografia atomica, ADR 0015 D9).
3. Validare senza rete: `scada-driver-modbus-tcp --config <driver.toml> --check-config --kernel-config <kernel.toml>`.
4. Un cambio della mappa = **nuova versione** del profilo (file nuovo), mai modifica in place di una versione in uso.
