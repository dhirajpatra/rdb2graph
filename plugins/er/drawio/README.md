# rdb2graph — draw.io / diagrams.net Parser Plugin
Parses `.drawio` XML files containing ER diagrams created in draw.io or diagrams.net.

Export: **File → Save** (native `.drawio` format)

## Install
No extra packages needed — uses Python stdlib `xml.etree`.

## config.yaml
```yaml
er_diagram:
  path: "./schema.drawio"
  format: "drawio"
```

## Shape styles supported
- `shape=table` — standard draw.io table shape
- `swimlane` — swimlane-based ER entities
- `shape=mxgraph.erd.*` — draw.io ERD shape library
- Edge cells with `source`/`target` for relationships

## Known limitations
- Column FK detection is heuristic (based on label text patterns like "FK", "FOREIGN")
- Relationship cardinality inferred from edge arrow style; may need manual review
- Nested diagrams (multiple pages) partially supported — all pages are parsed

## Contributing
Open a PR — see the root [README contributing guide](../../../../README.md#contributing).
