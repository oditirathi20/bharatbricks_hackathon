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
- `POST /check-eligibility`

Example request for `POST /check-eligibility`:

```json
{
  "income": 300000,
  "occupation": "farmer",
  "land_acres": 1.5,
  "category": "OBC"
}
```

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

## Databricks Pipeline Run Order

Run notebooks/scripts in this exact sequence:

1. `databricks/01_bronze_citizens.py`
2. `databricks/02_silver_processing.py`
3. `databricks/03_schemes_engine.py`
4. `databricks/04_eligibility_matching.py`
5. `databricks/05_adhikar_certificates.py`
6. `databricks/06_automation_triggers.py`

## Required Test Coverage

The scripts include these validation checks:

1. Test 1 (Synthetic Data Generation):
  - In `01_bronze_citizens.py`
  - Verifies at least 50 rows
  - Verifies farmer distribution and low/high income range
2. Test 2 (Matching Logic):
  - In `04_eligibility_matching.py`
  - Uses profile `{income: 300000, occupation: farmer, land_acres: 1.5, category: OBC}`
  - Verifies at least one eligible scheme
3. Test 3 (Edge Cases):
  - In `04_eligibility_matching.py`
  - Verifies no-match profile, missing values, and extreme income handling
4. Test 4 (New Scheme Trigger):
  - In `06_automation_triggers.py`
  - Appends new scheme, reruns eligibility for new schemes, verifies new eligibility

## Example Validation Output

Expected console-style outputs (illustrative):

```text
[TEST-1] Synthetic generation passed
[TEST-2] Matching logic passed with eligible schemes=2
[TEST-3] Edge cases passed (no-match, missing values, extreme income)
[TEST-4] New scheme trigger passed. inserted_scheme_id=SCH-NEW-20260328153045
```

## GitHub + Databricks Import Notes

1. Push this repo to GitHub (already connected to `oditirathi20/bharatbricks_hackathon`).
2. In Databricks Workspace, create a repo and connect to the same GitHub repository.
3. Import or open notebook scripts from `databricks/` in sequence.
4. Configure Job workflows to execute scripts based on your schedule.
