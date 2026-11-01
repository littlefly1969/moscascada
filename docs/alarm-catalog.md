# Catalogo allarmi (VS-005, ADR 0012) — interfaccia, non RAM critica

Tre componenti separate, collegate dalla chiave stabile `alarm_id` (u32 ≠ 0, unico nell'impianto):

| Componente | Dove vive | Contenuto | Nel digest del kernel |
|---|---|---|---|
| **Alarm State Table** | `scada-core` (RAM, `AlarmState` 32 B per regola) | flag active/unacked/input_valid/armed, occurrence, token, ultimo evento | sì (v3) |
| **Regole** | configurazione del kernel (`[[alarms.rules]]`) | tag, kind, soglie, severità, nome tecnico | sì (fingerprint v3) |
| **Alarm Catalog** | asset versionato **esterno** (HMI / catalog service futuro) | testi multilingua, istruzioni, cause, suggerimenti, asset, documenti, sinottiche | **no** |
| **Operator Action Log** | VS-006 (durevole) | note, allegati, ACK con principal umano | — |

Il kernel **non legge mai** il catalogo: nessuna stringa, URL o blob nel percorso caldo, nei record dell'outbox o nella cella di stato. Il catalogo ha un proprio `catalog_version` e può cambiare senza toccare digest o fingerprint, a parità di regole. Severità, tag e soglie **non** stanno nel catalogo (lo schema rifiuta chiavi sconosciute nelle voci): sono semantica del kernel.

## Schema `AlarmCatalogEntry` (versione 1)

Schema JSON completo: [`catalog/alarm-catalog.schema.json`](catalog/alarm-catalog.schema.json) (draft 2020-12).

```text
AlarmCatalog {
  catalog_schema_version: 1,
  catalog_version: String,
  config_fingerprint: hex | "unbound",      // configurazione per cui è stato scritto
  languages: [lang],
  assets:    [{ asset_id, title, parent_asset_id? }],      // gerarchia macchine/impianto
  graphics:  [{ graphic_id, title }],                       // sinottiche
  documents: [{ document_id, title, uri, kind }],           // procedure, manuali, schemi
  alarms:    [AlarmCatalogEntry],
}
AlarmCatalogEntry {
  alarm_id: u32,                                            // FK -> [[alarms.rules]].id
  title, description?, operator_instructions?: Text,        // Text = { lang: String }
  causes?, suggestions?: [Text],
  related_tag_ids?: [u32],                                  // N:M allarmi <-> tag
  asset_ids?: [asset_id],                                   // N:M allarmi <-> asset
  document_ids?: [document_id],                             // N:M allarmi <-> documenti
  graphics?: [AlarmGraphicRef],                             // 1:N verso le sinottiche
}
AlarmGraphicRef { graphic_id, element_id?: String, action: Open | OpenAndHighlight }
```

Relazioni: un allarme → più sinottiche/elementi (1:N); allarmi ↔ tag, asset e documenti (N:M: lo stesso documento serve più allarmi, un allarme cita più documenti). Le chiavi future delle azioni operatore sono `(kernel_instance_id, alarm_id, occurrence_id, event_seq)` (VS-006).

## Esempio validato

[`catalog/alarm-catalog.example.json`](catalog/alarm-catalog.example.json), scritto per [`config/examples/alarms.toml`](../config/examples/alarms.toml). Verificato in `scripts/check.sh` da:

```bash
python3 scripts/check_alarm_catalog.py docs/catalog/alarm-catalog.example.json config/examples/alarms.toml
```

che controlla schema essenziale (campi obbligatori, enum `action`, pattern degli id, lingue dichiarate, nessuna chiave semantica nelle voci) e integrità referenziale (ogni `alarm_id` esiste nelle regole, ogni tag in `[[tags]]`, ogni asset/documento/sinottica dichiarato). Il resolver HMI (apertura sinottica, evidenziazione elemento) non fa parte di VS-005.

## Compatibilità futura

- Campi nuovi: solo opzionali, `catalog_schema_version` invariato; un campo obbligatorio nuovo o un cambio di significato ⇒ versione 2.
- Un `alarm_id` è stabile per sempre: rinominare la regola (`name`) cambia il fingerprint del kernel, non la chiave del catalogo.
