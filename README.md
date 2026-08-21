# bi_v2

Proyecto ETL migrado a `uv`.

## Comandos

```bash
uv sync
uv run bi-etl
uv run bi-etl-cli list
uv run python -m unittest discover -s tests
```

## Compatibilidad

`requirements.txt` se mantiene de momento para no romper integraciones existentes, pero la fuente principal de dependencias pasa a ser `pyproject.toml`.
