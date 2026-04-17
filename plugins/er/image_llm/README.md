# rdb2graph — Image → LLM Vision Parser Plugin
Extracts ER schema from a PNG/JPG/WEBP screenshot using a vision-capable LLM.
Supports Claude (Anthropic) and GPT-4o (OpenAI) backends.

## Install
```bash
# For Claude backend (default):
pip install anthropic>=0.25.0

# For OpenAI GPT-4o backend:
pip install openai>=1.0.0
```

## config.yaml
```yaml
er_diagram:
  path: "./schema_screenshot.png"
  format: "image_llm"
  llm_backend: "claude"              # claude | openai
  llm_model: "claude-opus-4-5"       # optional model override
  api_key_env: "ANTHROPIC_API_KEY"   # env var name for the API key
```

## Environment variable
```bash
export ANTHROPIC_API_KEY="sk-ant-..."
# or
export OPENAI_API_KEY="sk-..."
```

## Tips for best results
- Use a high-resolution screenshot (at least 1200px wide)
- Ensure all table names, column names, and relationship lines are clearly visible
- Dark/busy backgrounds may reduce accuracy — use a light diagram theme if possible
- Claude Opus gives the best extraction accuracy for complex diagrams

## Known limitations
- Accuracy depends on image quality and diagram clarity
- Very large diagrams (50+ tables) may exceed token limits — split into sections
- Hand-drawn or non-standard diagram styles may produce incomplete results
- API calls incur cost — run with `--only er_parse` first to validate before full pipeline

## Contributing
Open a PR — see the root [README contributing guide](../../../../README.md#contributing).
