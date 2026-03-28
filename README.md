# Adhikar-Aina: Frontend + Backend + Databricks

This repository is organized as a production-style hackathon stack:

- `frontend/`: React + Vite + Tailwind application
- `backend/`: FastAPI integration layer for frontend to Databricks
- `databricks/`: Databricks notebook scripts (bronze/silver/schemes/eligibility/certificates/automation)

## Folder Layout

```text
bharatbricks/
  frontend/
  backend/
  databricks/
```

## Databricks Notebooks

Use these files in order when importing/running on Databricks:

1. `databricks/01_bronze_citizens.py`
2. `databricks/02_silver_processing.py`
3. `databricks/03_schemes_engine.py`
4. `databricks/04_eligibility_matching.py`
5. `databricks/05_adhikar_certificates.py`
6. `databricks/06_automation_triggers.py`

## Backend Setup

1. Create a virtual environment and install dependencies.

```powershell
cd backend
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

2. Create environment file from template.

```powershell
copy .env.example .env
```

3. Set Databricks values in `.env`:

- `DATABRICKS_SERVER_HOSTNAME`
- `DATABRICKS_HTTP_PATH`
- `DATABRICKS_ACCESS_TOKEN`
- Optional: `DATABRICKS_CATALOG`, `DATABRICKS_SCHEMA`

4. Run backend API.

```powershell
uvicorn app:app --reload --host 0.0.0.0 --port 8000
```

API endpoints:

- `POST /api/register-user`
- `GET /api/get-results/{citizen_id}`
- `GET /health`

## Frontend Setup

1. Install dependencies and run app.

```powershell
cd frontend
npm install
npm run dev
```

2. Optional frontend env (`frontend/.env`):

```env
VITE_API_BASE_URL=http://127.0.0.1:8000
```

## Integration Flow

1. Frontend submits onboarding profile to backend `POST /api/register-user`.
2. Backend writes citizen profile to Databricks bronze table.
3. Frontend requests results using `GET /api/get-results/{citizen_id}`.
4. Backend returns eligibility results from Databricks.

## GitHub + Databricks Import Notes

1. Push this repo to GitHub (already connected to `oditirathi20/bharatbricks_hackathon`).
2. In Databricks Workspace, create a repo and connect to the same GitHub repository.
3. Import or open notebook scripts from `databricks/` in sequence.
4. Configure Job workflows to execute scripts based on your schedule.
