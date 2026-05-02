# FSearch

Search and export file metadata from:

- Local Google Drive Desktop synced folders (**Local mode**)
- Google Drive API using OAuth (**OAuth mode**)

Results can be exported to CSV/JSON/TXT and appended to Google Sheets.

## Requirements

- Python 3.10+

Install dependencies:

```bash
pip install gspread google-auth google-api-python-client google-auth-oauthlib google-auth-httplib2
```

## Google Sheets Setup (for upload)

This project uploads to Google Sheets using a **service account**.

1. Create a Service Account in Google Cloud and download the JSON key.
2. Save it as `service-account.json` in the project folder.
3. Share your target Google Sheet with the **service account email** as Editor.

## OAuth Setup (for Drive API mode)

1. Enable **Google Drive API** in Google Cloud.
2. Create OAuth Client ID: **Desktop application**.
3. Download the credentials JSON and save it as `credentials.json`.
4. First OAuth run will open a browser and generate `token.pickle`.

## Usage

Run interactively:

```bash
python DriveSearch.py
```

You will be prompted for:

- Target: Clients / Consultants / Both
- Mode: Local / OAuth
- Duplicate check: Yes / No

### With stats

```bash
python DriveSearch.py --stats
```

This also appends a summary row to:

- `LSheet_STATS` (local runs)
- `ASheet_STATS` (oauth runs)

## Sensitive files

These files are **ignored by git** (see `.gitignore`):

- `credentials.json`
- `service-account.json`
- `token.pickle`

Do not commit them.
