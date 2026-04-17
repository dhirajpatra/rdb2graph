# rdb2graph — Lucidchart Parser Plugin
Parses Lucidchart ER diagrams. Supports two export formats: CSV and VSDX.

## Export options

| Method | Format | Quality |
|--------|--------|---------|
| File → Export → CSV | `.csv` | Good — structured text |
| File → Export → Visio | `.vsdx` | Best effort — XML extraction |

## Install
```bash
# CSV mode: no extra packages needed
# VSDX mode:
pip install lxml>=4.9.0
```

## config.yaml
```yaml
er_diagram:
  path: "./lucidchart_export.csv"   # or .vsdx
  format: "lucidchart"
```

## Known limitations
- CSV column details are embedded in "Text Area" fields; formatting varies by template
- VSDX parsing is best-effort; FK columns not always detectable
- Relationship cardinality from CSV requires "Line Source"/"Line Destination" columns

## Contributing
Open a PR — see the root [README contributing guide](../../../../README.md#contributing).
