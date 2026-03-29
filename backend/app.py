import json
import os
import time
import uuid
import hashlib
from typing import Any, Dict, List, Optional

import requests
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Response
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from databricks import sql

load_dotenv()

# ENV VARIABLES
BRONZE_TABLE = os.getenv("DATABRICKS_BRONZE_TABLE", "bronze_citizens")
RESULTS_TABLE = os.getenv("DATABRICKS_RESULTS_TABLE", "eligibility_results")

DATABRICKS_INSTANCE = os.getenv("DATABRICKS_INSTANCE")
TOKEN = os.getenv("DATABRICKS_TOKEN")
JOB_ID = int(os.getenv("DATABRICKS_JOB_ID", "0"))

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
OPENAI_TTS_MODEL = os.getenv("OPENAI_TTS_MODEL", "gpt-4o-mini-tts")
OPENAI_TTS_VOICE = os.getenv("OPENAI_TTS_VOICE", "alloy")
OPENAI_TTS_URL = os.getenv("OPENAI_TTS_URL", "https://api.openai.com/v1/audio/speech")
OPENAI_CHAT_MODEL = os.getenv("OPENAI_CHAT_MODEL", "gpt-4.1-mini")
OPENAI_RESPONSES_URL = os.getenv("OPENAI_RESPONSES_URL", "https://api.openai.com/v1/responses")

LANGUAGE_NAME_BY_CODE = {
    "as": "Assamese",
    "bn": "Bengali",
    "bodo": "Bodo",
    "doi": "Dogri",
    "en": "English",
    "gu": "Gujarati",
    "hi": "Hindi",
    "kn": "Kannada",
    "kok": "Konkani",
    "ks": "Kashmiri",
    "mai": "Maithili",
    "ml": "Malayalam",
    "mni": "Manipuri (Meitei)",
    "mr": "Marathi",
    "ne": "Nepali",
    "or": "Odia",
    "pa": "Punjabi",
    "sa": "Sanskrit",
    "sat": "Santhali",
    "sd": "Sindhi",
    "ta": "Tamil",
    "te": "Telugu",
    "ur": "Urdu",
}

# ---------- DB CONNECTION ----------

def _connection():
    return sql.connect(
        server_hostname=os.getenv("DATABRICKS_SERVER_HOSTNAME"),
        http_path=os.getenv("DATABRICKS_HTTP_PATH"),
        access_token=os.getenv("DATABRICKS_ACCESS_TOKEN"),
    )

def _catalog():
    return os.getenv("DATABRICKS_CATALOG", "workspace")

def _schema():
    return os.getenv("DATABRICKS_SCHEMA", "default")

def _table(name):
    return f"{_catalog()}.{_schema()}.{name}"

def _sql_literal(value):
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)):
        return str(value)
    safe = str(value).replace("'", "''")
    return f"'{safe}'"


def _extract_response_text(data: Dict[str, Any]) -> str:
    output_text = data.get("output_text")
    if isinstance(output_text, str) and output_text.strip():
        return output_text.strip()

    for item in data.get("output", []):
        text = item.get("text") if isinstance(item, dict) else None
        if isinstance(text, str) and text.strip():
            return text.strip()
        for content_item in item.get("content", []) if isinstance(item, dict) else []:
            if isinstance(content_item, dict):
                inner = content_item.get("text") or content_item.get("output_text")
                if isinstance(inner, str) and inner.strip():
                    return inner.strip()
    return ""


def _table_columns(table_name: str) -> List[str]:
    with _connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f"DESCRIBE TABLE {_table(table_name)}")
            rows = cur.fetchall()

    columns: List[str] = []
    for row in rows:
        col_name = str(row[0]).strip() if row and row[0] else ""
        if not col_name or col_name.startswith("#"):
            continue
        # Stop before partition/info section in DESCRIBE output
        if col_name.lower().startswith("partitioning"):
            break
        columns.append(col_name)
    return columns


def _normalize_bool(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    text = str(value).strip().lower()
    if text in {"true", "1", "yes", "y"}:
        return True
    if text in {"false", "0", "no", "n"}:
        return False
    return default


def _income_bracket(income: float) -> str:
    if income < 50000:
        return "EWS"
    if income < 100000:
        return "LIG"
    if income < 200000:
        return "MIG"
    return "HIG"


def _land_category(land_acres: float) -> str:
    if land_acres < 1.0:
        return "marginal"
    if land_acres < 2.5:
        return "small"
    if land_acres < 5.0:
        return "medium"
    return "large"


def _occupation_category(occupation: str) -> str:
    text = str(occupation or "").strip().lower()
    if any(token in text for token in ["farmer", "farm", "agri", "cultivator"]):
        return "agriculture"
    if any(token in text for token in ["student", "school", "college"]):
        return "education"
    if any(token in text for token in ["business", "shop", "trader", "startup"]):
        return "business"
    if any(token in text for token in ["worker", "labour", "daily", "employment"]):
        return "employment"
    return "general"


def build_eligibility_explanation(user: Dict[str, Any]) -> Dict[str, str]:
    income = float(user.get("annual_income", user.get("income", 0)) or 0)
    land = float(user.get("land_acres", 0) or 0)
    occupation = str(user.get("occupation", ""))
    return {
        "income_bracket": _income_bracket(income),
        "occupation_category": _occupation_category(occupation),
        "land_category": _land_category(land),
    }


def ensure_mapping_table_exists() -> None:
    query = f"""
        CREATE TABLE IF NOT EXISTS {_table('telegram_user_mapping')} (
            citizen_id STRING,
            telegram_chat_id STRING,
            telegram_username STRING,
            updated_at TIMESTAMP
        )
    """
    with _connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query)


def save_telegram_mapping(citizen_id: str, telegram_chat_id: str, telegram_username: Optional[str] = None) -> None:
    ensure_mapping_table_exists()

    username_sql = _sql_literal(telegram_username or "")
    query = f"""
        MERGE INTO {_table('telegram_user_mapping')} AS target
        USING (
            SELECT
                {_sql_literal(citizen_id)} AS citizen_id,
                {_sql_literal(str(telegram_chat_id))} AS telegram_chat_id,
                {username_sql} AS telegram_username,
                current_timestamp() AS updated_at
        ) AS source
        ON target.citizen_id = source.citizen_id
        WHEN MATCHED THEN UPDATE SET
            telegram_chat_id = source.telegram_chat_id,
            telegram_username = source.telegram_username,
            updated_at = source.updated_at
        WHEN NOT MATCHED THEN INSERT (citizen_id, telegram_chat_id, telegram_username, updated_at)
        VALUES (source.citizen_id, source.telegram_chat_id, source.telegram_username, source.updated_at)
    """

    with _connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query)


def infer_bronze_fields_with_gpt(user: Dict[str, Any]) -> Dict[str, Any]:
    fallback = {
        "district": "Satara",
        "taluka": "Karad",
        "village": "Umbraj",
        "ward_no": 1,
        "survey_no": "NA/1",
        "housing_status": user.get("housing_status") or "semi_pucca",
        "employment_days": user.get("employment_days") or 0,
        "is_tribal": user.get("is_tribal") or False,
        "has_bpl_card": False,
        "has_electricity": True,
        "has_water_source": True,
        "data_source": "frontend_gpt_enriched",
    }

    if not OPENAI_API_KEY:
        return fallback

    prompt = (
        "Given this citizen intake JSON, infer missing bronze-layer profile fields for Indian welfare data. "
        "Return strict JSON object only with keys: district, taluka, village, ward_no, survey_no, "
        "housing_status, employment_days, is_tribal, has_bpl_card, has_electricity, has_water_source, data_source. "
        "Use realistic defaults if uncertain.\n\n"
        f"Input JSON: {json.dumps(user)}"
    )

    try:
        response = requests.post(
            OPENAI_RESPONSES_URL,
            headers={
                "Authorization": f"Bearer {OPENAI_API_KEY}",
                "Content-Type": "application/json",
            },
            json={
                "model": OPENAI_CHAT_MODEL,
                "input": [
                    {
                        "role": "system",
                        "content": [
                            {
                                "type": "input_text",
                                "text": "You are a strict JSON generator for welfare profile normalization.",
                            }
                        ],
                    },
                    {
                        "role": "user",
                        "content": [{"type": "input_text", "text": prompt}],
                    },
                ],
                "text": {"format": {"type": "json_object"}},
            },
            timeout=25,
        )
        response.raise_for_status()
        text = _extract_response_text(response.json())
        parsed = json.loads(text) if text else {}
        if not isinstance(parsed, dict):
            return fallback
        merged = {**fallback, **parsed}
        return merged
    except Exception:
        return fallback

# ---------- CORE FUNCTIONS ----------

def append_user_to_bronze(user: Dict[str, Any]) -> str:
    citizen_id = str(user.get("citizen_id") or f"TEST-{uuid.uuid4().hex[:6]}").strip()
    income_value = user.get("income", user.get("annual_income", 0))
    caste_value = user.get("caste_category") or user.get("category") or "GEN"
    gpt_fields = infer_bronze_fields_with_gpt(user)

    bronze_record = {
        "citizen_id": citizen_id,
        "aadhaar_hash": hashlib.sha256(citizen_id.encode("utf-8")).hexdigest(),
        "district": gpt_fields.get("district", "Satara"),
        "taluka": gpt_fields.get("taluka", "Karad"),
        "village": gpt_fields.get("village", "Umbraj"),
        "ward_no": int(gpt_fields.get("ward_no", 1) or 1),
        "survey_no": str(gpt_fields.get("survey_no", "NA/1")),
        "land_acres": float(user.get("land_acres", 0) or 0),
        "annual_income": float(income_value or 0),
        "income": float(income_value or 0),
        "caste_category": str(caste_value).upper(),
        "category": str(caste_value).upper(),
        "is_tribal": _normalize_bool(gpt_fields.get("is_tribal"), False),
        "has_girl_child": _normalize_bool(user.get("has_girl_child"), False),
        "has_bpl_card": _normalize_bool(gpt_fields.get("has_bpl_card"), False),
        "housing_status": gpt_fields.get("housing_status", user.get("housing_status") or "semi_pucca"),
        "has_electricity": _normalize_bool(gpt_fields.get("has_electricity"), True),
        "has_water_source": _normalize_bool(gpt_fields.get("has_water_source"), True),
        "employment_days": int(gpt_fields.get("employment_days", user.get("employment_days", 0)) or 0),
        "occupation": user.get("occupation", ""),
        "data_source": gpt_fields.get("data_source", "frontend_live_test"),
    }

    existing_columns = _table_columns(BRONZE_TABLE)
    selected_columns = [col for col in bronze_record.keys() if col in existing_columns]
    if not selected_columns:
        raise ValueError(f"No compatible columns found in bronze table {_table(BRONZE_TABLE)}")

    if "created_at" in existing_columns:
        selected_columns.append("created_at")
    if "updated_at" in existing_columns:
        selected_columns.append("updated_at")

    values_sql: List[str] = []
    for col in selected_columns:
        if col in {"created_at", "updated_at"}:
            values_sql.append("current_timestamp()")
        else:
            values_sql.append(_sql_literal(bronze_record.get(col)))

    query = (
        f"INSERT INTO {_table(BRONZE_TABLE)} ({', '.join(selected_columns)}) "
        f"VALUES ({', '.join(values_sql)})"
    )

    with _connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query)

    return citizen_id


def trigger_databricks_job():
    url = f"{DATABRICKS_INSTANCE}/api/2.1/jobs/run-now"

    print("\n==== JOB TRIGGER DEBUG ====")
    print("URL:", url)
    print("JOB_ID:", JOB_ID)
    print("TOKEN START:", TOKEN[:10])  # sanity check

    response = requests.post(
        url,
        headers={
            "Authorization": f"Bearer {TOKEN}",
            "Content-Type": "application/json"
        },
        json={"job_id": JOB_ID},
    )

    print("STATUS:", response.status_code)
    print("RESPONSE:", response.text)
    print("===========================\n")

    if response.status_code != 200:
        raise Exception(f"Job trigger failed: {response.text}")

    payload = response.json()
    run_id = payload.get("run_id")
    if not run_id:
        raise Exception(f"Job trigger did not return run_id: {payload}")
    return int(run_id)


def wait_for_databricks_job(run_id: int, timeout_seconds: int = 420, poll_seconds: int = 8) -> None:
    url = f"{DATABRICKS_INSTANCE}/api/2.1/jobs/runs/get"
    started = time.time()

    while time.time() - started < timeout_seconds:
        response = requests.get(
            url,
            headers={"Authorization": f"Bearer {TOKEN}"},
            params={"run_id": run_id},
            timeout=30,
        )

        if response.status_code != 200:
            raise Exception(f"Failed fetching run status: {response.text}")

        state = response.json().get("state", {})
        life_cycle_state = state.get("life_cycle_state")
        result_state = state.get("result_state")

        if life_cycle_state == "TERMINATED":
            if result_state == "SUCCESS":
                return
            raise Exception(f"Databricks job failed: {state}")

        if life_cycle_state in {"INTERNAL_ERROR", "SKIPPED"}:
            raise Exception(f"Databricks job ended unexpectedly: {state}")

        time.sleep(poll_seconds)

    raise TimeoutError(f"Databricks job run_id={run_id} timed out after {timeout_seconds}s")


def fetch_results(citizen_id: str):
    time.sleep(15)  # safer wait

    query = f"""
        SELECT scheme_name, benefit
        FROM {_table(RESULTS_TABLE)}
        WHERE citizen_id = {_sql_literal(citizen_id)}
          AND eligibility_status = TRUE
    """

    with _connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            rows = cur.fetchall()
            columns = [col[0] for col in cur.description]

    return [dict(zip(columns, row)) for row in rows]

# ---------- API ----------

class CheckEligibilityRequest(BaseModel):
    citizen_id: Optional[str] = None
    income: Optional[float] = 0
    annual_income: Optional[float] = None
    occupation: Optional[str] = ""
    land_acres: Optional[float] = 0
    category: Optional[str] = "GEN"
    caste_category: Optional[str] = None
    has_girl_child: Optional[bool] = False
    state: Optional[str] = ""
    housing_status: Optional[str] = ""
    employment_days: Optional[int] = 0
    is_tribal: Optional[bool] = False


class TTSRequest(BaseModel):
    text: str
    language: Optional[str] = "en"


class TelegramLinkRequest(BaseModel):
    citizen_id: str
    telegram_chat_id: str
    telegram_username: Optional[str] = None


app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.post("/check-eligibility")
def check_eligibility(payload: CheckEligibilityRequest):
    user = payload.dict(exclude_none=True)
    if "caste_category" not in user and "category" in user:
        user["caste_category"] = user.get("category")
    if "annual_income" not in user:
        user["annual_income"] = user.get("income", 0)

    run_id: Optional[int] = None
    citizen_id = str(user.get("citizen_id") or f"TEST-{uuid.uuid4().hex[:6]}").strip()
    user["citizen_id"] = citizen_id
    explanation = build_eligibility_explanation(user)

    try:
        print("Appending user...")
        citizen_id = append_user_to_bronze(user)

        print("Triggering job...")
        run_id = trigger_databricks_job()

        print(f"Waiting for job completion... run_id={run_id}")
        wait_for_databricks_job(run_id)

        print("Fetching results...")
        results = fetch_results(citizen_id)

        return {
            "citizen_id": citizen_id,
            "run_id": run_id,
            "eligible_schemes": results,
            "eligibility_explanation": explanation,
        }

    except Exception as e:
        # Stable demo response contract: always return citizen_id, run_id, eligible_schemes.
        return {
            "citizen_id": citizen_id,
            "run_id": run_id,
            "eligible_schemes": [],
            "eligibility_explanation": explanation,
            "error": str(e),
        }


@app.post("/api/link-telegram")
def link_telegram(payload: TelegramLinkRequest):
    try:
        save_telegram_mapping(
            citizen_id=payload.citizen_id,
            telegram_chat_id=payload.telegram_chat_id,
            telegram_username=payload.telegram_username,
        )
        return {"ok": True, "citizen_id": payload.citizen_id, "telegram_chat_id": payload.telegram_chat_id}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/tts")
def text_to_speech(payload: TTSRequest):
    if not OPENAI_API_KEY:
        raise HTTPException(status_code=500, detail="OPENAI_API_KEY is missing on backend")

    text = (payload.text or "").strip()
    if not text:
        raise HTTPException(status_code=400, detail="Text is required")

    language_code = (payload.language or "en").lower()
    language_name = LANGUAGE_NAME_BY_CODE.get(language_code, "English")

    # Give the model a language hint while preserving the original question text.
    tts_input = f"Speak this in {language_name}: {text}"

    response = requests.post(
        OPENAI_TTS_URL,
        headers={
            "Authorization": f"Bearer {OPENAI_API_KEY}",
            "Content-Type": "application/json",
        },
        json={
            "model": OPENAI_TTS_MODEL,
            "voice": OPENAI_TTS_VOICE,
            "input": tts_input,
            "format": "mp3",
        },
        timeout=45,
    )

    if response.status_code != 200:
        raise HTTPException(status_code=502, detail=f"OpenAI TTS failed: {response.text}")

    return Response(content=response.content, media_type="audio/mpeg")