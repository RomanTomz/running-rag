# Running RAG

Running RAG ingests your Garmin Connect activities, builds concise summaries, and
stores them in a local Chroma vector database so you can ask natural-language
questions about your training history.

## 1. Prerequisites

- Python 3.11+
- [uv](https://docs.astral.sh/uv/) (recommended) or `pip`
- An OpenAI API key with access to the `text-embedding-3-small` and
  `gpt-4o-mini` models
- Garmin Connect credentials (email/username + password)

Install dependencies:

```bash
uv sync   # or: pip install -r requirements.txt (generated via `uv pip compile`)
```

## 2. Environment variables

Create a `.env` file in the project root with at least the following values:

```dotenv
OPENAI_API_KEY=sk-your-key
# Provide either GARMIN_EMAIL or GARMIN_USERNAME
GARMIN_EMAIL=you@example.com
GARMIN_USERNAME=
GARMIN_PASSWORD=super-secret
# Optional: override where you save Garmin CSV exports
GARMIN_DATA_DIR=garmin_data
```

Notes:

- `OPENAI_API_KEY` is required for both ingestion (to embed summaries) and
  querying (to embed questions + call the LLM).
- `GARMIN_EMAIL` and `GARMIN_PASSWORD` (or `GARMIN_USERNAME`) are used only by
  `ingest/ingest_garminconnect.py` when downloading activities directly from
  Garmin.
- The ingestion + query scripts call `python-dotenv`, so the `.env` file is
  loaded automatically.

## 3. Typical workflow

1. **Download Garmin activities** (optional if you already have CSV exports):
   ```bash
   uv run python ingest/ingest_garminconnect.py
   ```
   This logs in once, pages through your activities, and prints a `pandas`
   summary. Save the dataframe as CSVs (e.g., via Jupyter or
   `df.to_csv("garmin_data/activities.csv", index=False)`).

2. **Ingest CSVs into Chroma**:
   ```bash
   uv run python ingest/ingest.py --data-dir garmin_data
   ```
   Each row is summarised via `summarise/summariser.py`, embedded with OpenAI,
   and persisted in `./chroma_db` (collection: `garmin_runs`). Use
   `--preview --preview-limit 5` to inspect summaries without writing data.

3. **Query your training log**:
   ```bash
   uv run python ask.py "How many tempo runs did I do last month?" --show-context
   ```
   `ask.py` uses `query.query()` to retrieve the top summaries and sends a chat
   completion request that produces a coach-style answer.

## 4. Data locations & reset instructions

- **Vector store**: embeddings + metadata live under `./chroma_db` (created by
  `chromadb.PersistentClient`). Delete this directory to reset the knowledge
  base and rerun ingestion.
- **Raw Garmin cache (optional)**: if you use `storage/sqlite_store.py` or other
  notebook utilities, SQLite files are stored in `./data/garmin.db`. Remove the
  file to rebuild from scratch.
- **CSV exports**: keep your source exports under `./garmin_data/` (or the path
  given by `GARMIN_DATA_DIR`). Removing or replacing these files does not alter
  previously embedded data until you delete `chroma_db`.

## 5. Summariser logic

The text used for embeddings is produced by
[`summarise/summariser.py`](summarise/summariser.py). If you want to tweak how
activities are described (e.g., add more metrics, change pacing rules, etc.),
edit `summarise_activity_row()` and `build_metadata()` there before re-running
`ingest/ingest.py`.

## 6. Troubleshooting

| Symptom | Likely cause | Fix |
| --- | --- | --- |
| `RuntimeError: Collection not found, run ingest.py first` | `query.py` could not open the `garmin_runs` collection because `chroma_db` is empty or was deleted. | Run `uv run python ingest/ingest.py ...` to rebuild the collection, or delete `./chroma_db` and ingest again. |
| `ValueError: Set GARMIN_EMAIL ...` | Garmin credentials missing from `.env`. | Ensure `GARMIN_EMAIL`/`GARMIN_USERNAME` and `GARMIN_PASSWORD` are set, then rerun the download script. |
| Unexpected embeddings/answers | Source CSVs missing fields or summariser output not what you expect. | Preview chunks via `uv run python ingest/ingest.py --preview --preview-limit 5` and adjust `summarise/summariser.py`. |
| Need a clean slate | Old data persists across runs. | Delete `./chroma_db` and/or `./data/garmin.db`, then rerun ingestion. |

For additional debugging, enable `--show-context` when running `ask.py` to see
exactly which summaries informed the final answer.
