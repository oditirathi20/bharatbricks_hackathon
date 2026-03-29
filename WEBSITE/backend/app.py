import json
import os
import time
import uuid
import hashlib
from typing import Any, Dict, List, Optional

import requests
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Response, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from openai import OpenAI

from databricks import sql

load_dotenv()

# ENV VARIABLES
BRONZE_TABLE = os.getenv("DATABRICKS_BRONZE_TABLE", "bronze_citizens")
SILVER_TABLE = os.getenv("DATABRICKS_SILVER_TABLE", "silver_citizens")
RESULTS_TABLE = os.getenv("DATABRICKS_RESULTS_TABLE", "eligibility_results")
SCHEMES_TABLE = os.getenv("DATABRICKS_SCHEMES_TABLE", "schemes_clean")

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

# Initialize OpenAI client
openai_client = OpenAI(api_key=OPENAI_API_KEY)

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
    """Trigger Databricks job to run eligibility matching."""
    if not JOB_ID or JOB_ID == 0:
        print("❌ ERROR: JOB_ID not configured. Cannot trigger job.")
        raise Exception(f"Databricks JOB_ID not set. Current: {JOB_ID}")
    
    url = f"{DATABRICKS_INSTANCE}/api/2.1/jobs/run-now"

    print("\n==== JOB TRIGGER ====")
    print(f"URL: {url}")
    print(f"JOB_ID: {JOB_ID}")

    try:
        response = requests.post(
            url,
            headers={
                "Authorization": f"Bearer {TOKEN}",
                "Content-Type": "application/json"
            },
            json={"job_id": JOB_ID},
            timeout=30,
        )

        print(f"Status: {response.status_code}")
        print(f"Response: {response.text}")
        
        if response.status_code != 200:
            error_msg = f"Job trigger failed with status {response.status_code}: {response.text}"
            print(f"❌ {error_msg}")
            raise Exception(error_msg)

        payload = response.json()
        run_id = payload.get("run_id")
        if not run_id:
            raise Exception(f"No run_id in response: {payload}")
        
        print(f"✅ Job triggered successfully. Run ID: {run_id}")
        print("====================\n")
        return int(run_id)
    except Exception as e:
        print(f"❌ Job trigger exception: {e}")
        import traceback
        traceback.print_exc()
        raise


def wait_for_databricks_job(run_id: int, timeout_seconds: int = 600, poll_seconds: int = 5) -> None:
    """Wait for Databricks job to complete."""
    url = f"{DATABRICKS_INSTANCE}/api/2.1/jobs/runs/get"
    started = time.time()
    
    print(f"⏳ Waiting for job run_id={run_id}...")

    while time.time() - started < timeout_seconds:
        try:
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
            
            print(f"  Status: {life_cycle_state} | Result: {result_state}")

            if life_cycle_state == "TERMINATED":
                if result_state == "SUCCESS":
                    print(f"✅ Job completed successfully!")
                    return
                raise Exception(f"Job failed: {state}")

            if life_cycle_state in {"INTERNAL_ERROR", "SKIPPED"}:
                raise Exception(f"Job ended unexpectedly: {state}")

            time.sleep(poll_seconds)
        except Exception as e:
            print(f"❌ Error waiting for job: {e}")
            raise

    raise TimeoutError(f"Job run_id={run_id} timed out after {timeout_seconds}s")


def retrieve_citizen_from_silver(citizen_id: str) -> Optional[Dict[str, Any]]:
    """Retrieve existing citizen data from silver_citizens table."""
    print(f"🔍 Looking up citizen: {citizen_id}")
    try:
        query = f"""
            SELECT citizen_id, district, annual_income, income_bracket,
                   occupation_category, land_acres, land_category, category,
                   has_daughter, citizen_tags
            FROM {_table(SILVER_TABLE)}
            WHERE citizen_id = {_sql_literal(citizen_id)}
            LIMIT 1
        """
        print(f"🔍 Query: {query}")
        
        conn = _connection()
        cursor = conn.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        
        if not rows:
            print(f"❌ No citizen found: {citizen_id}")
            cursor.close()
            conn.close()
            return None
            
        columns = [col[0] for col in cursor.description]
        citizen_data = dict(zip(columns, rows[0]))
        
        cursor.close()
        conn.close()
        
        print(f"✅ Retrieved citizen from silver layer: {citizen_id}")
        print(f"   Income bracket: {citizen_data.get('income_bracket')}")
        print(f"   District: {citizen_data.get('district')}")
        return citizen_data
    except Exception as e:
        print(f"❌ Databricks lookup error: {e}")
        import traceback
        traceback.print_exc()
        return None


def fetch_results(citizen_id: str, limit: Optional[int] = None):
    """Fetch eligibility results from Databricks results table."""
    print(f"🔍 Fetching results for citizen: {citizen_id}")
    
    query = f"""
        SELECT scheme_name, benefit
        FROM {_table(RESULTS_TABLE)}
        WHERE citizen_id = {_sql_literal(citizen_id)}
          AND eligibility_status = TRUE
    """

    try:
        conn = _connection()
        cursor = conn.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        columns = [col[0] for col in cursor.description]
        cursor.close()
        conn.close()

        results = [dict(zip(columns, row)) for row in rows]
        print(f"✅ Found {len(results)} eligible schemes for {citizen_id}")
        
        if limit:
            return results[:limit]
        return results
    except Exception as e:
        print(f"❌ Error fetching results: {e}")
        import traceback
        traceback.print_exc()
        raise

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
    limit: Optional[int] = 5  # Pagination: number of schemes to return


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
    limit = user.pop("limit", 5)  # Extract pagination limit
    
    if "caste_category" not in user and "category" in user:
        user["caste_category"] = user.get("category")
    if "annual_income" not in user:
        user["annual_income"] = user.get("income", 0)

    run_id: Optional[int] = None
    citizen_id = str(user.get("citizen_id") or "").strip()
    
    # If citizen_id provided, try to retrieve existing data from silver table
    if citizen_id:
        existing = retrieve_citizen_from_silver(citizen_id)
        if existing:
            print(f"✅ Found existing citizen data for {citizen_id}")
            # Pre-fill with existing data
            user = {**existing, **user}  # User input overrides DB data
        else:
            print(f"ℹ️  No existing data for citizen {citizen_id} - will create new record")
    else:
        citizen_id = f"TEST-{uuid.uuid4().hex[:6]}"
    
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
        results = fetch_results(citizen_id, limit=limit)

        return {
            "citizen_id": citizen_id,
            "run_id": run_id,
            "eligible_schemes": results,
            "eligibility_explanation": explanation,
        }

    except Exception as e:
        # Stable demo response contract: always return citizen_id, run_id, eligible_schemes.
        error_msg = str(e)
        print(f"❌ ERROR: {error_msg}")
        import traceback
        print(traceback.format_exc())
        return {
            "citizen_id": citizen_id,
            "run_id": run_id,
            "eligible_schemes": [],
            "eligibility_explanation": explanation,
            "error": error_msg,
        }


@app.get("/api/citizen/{citizen_id}")
def get_citizen_info(citizen_id: str):
    """Retrieve existing citizen information from silver layer."""
    try:
        citizen_data = retrieve_citizen_from_silver(citizen_id)
        if citizen_data:
            return {
                "found": True,
                "citizen": citizen_data,
            }
        else:
            return {
                "found": False,
                "message": f"No record found for citizen ID: {citizen_id}",
            }
    except Exception as e:
        print(f"❌ Error retrieving citizen: {e}")
        import traceback
        print(traceback.format_exc())
        return {
            "found": False,
            "error": str(e),
        }


@app.get("/api/schemes/{citizen_id}")
def get_citizen_schemes(citizen_id: str, limit: int = 5, offset: int = 0):
    """
    Get eligible schemes for a citizen with pagination support.
    
    Args:
        citizen_id: The citizen ID
        limit: Number of schemes to return (default 5)
        offset: Number of schemes to skip (default 0, for pagination)
    """
    try:
        # For demo mode, return all schemes with pagination
        all_results = fetch_results(citizen_id, limit=None)
        total = len(all_results)
        paginated = all_results[offset:offset + limit]
        
        return {
            "citizen_id": citizen_id,
            "total_schemes": total,
            "returned_count": len(paginated),
            "offset": offset,
            "limit": limit,
            "has_more": (offset + limit) < total,
            "schemes": paginated,
        }
    except Exception as e:
        print(f"❌ Error fetching schemes: {e}")
        import traceback
        print(traceback.format_exc())
        return {
            "error": str(e),
            "schemes": [],
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


@app.post("/api/stt")
async def speech_to_text(file: UploadFile = File(...)):
    """Convert speech (audio) to text using OpenAI Whisper API.
    
    Args:
        file: Audio file to transcribe (WAV, MP3, M4A, FLAC, etc.)
        
    Returns:
        JSON with transcribed text from audio
    """
    try:
        if not OPENAI_API_KEY:
            raise HTTPException(
                status_code=500,
                detail="OPENAI_API_KEY environment variable is not set"
            )
        
        # Read the audio file content
        audio_content = await file.read()
        
        if not audio_content:
            raise HTTPException(status_code=400, detail="Audio file is empty")
        
        # Call OpenAI Whisper API using the client
        transcript = openai_client.audio.transcriptions.create(
            model="whisper-1",
            file=(file.filename or "audio.wav", audio_content, file.content_type or "audio/wav"),
            response_format="json",
            timeout=45.0,
        )
        
        transcribed_text = transcript.text.strip() if transcript.text else ""
        
        if not transcribed_text:
            print(f"Warning: Empty transcription for file {file.filename}")
        
        return {
            "ok": True,
            "text": transcribed_text,
            "transcript": transcribed_text,
        }
    
    except HTTPException:
        raise
    except Exception as e:
        error_msg = str(e)
        print(f"STT Error: {error_msg}")
        raise HTTPException(status_code=500, detail=f"STT error: {error_msg}")


# Certificate Translations
CERTIFICATE_TRANSLATIONS = {
    "hi": {
        "title": "⚖️ अधिकार प्रमाणपत्र",
        "subtitle": "पात्रता और अधिकार प्रमाणपत्र",
        "subsubtitle": "सरकारी कल्याण योजना लाभ",
        "cert_id": "प्रमाणपत्र ID",
        "generated": "जारी",
        "eligible": "पात्रता की पुष्टि",
        "certifies": "यह प्रमाणित करता है कि:",
        "citizen_id": "नागरिक ID",
        "scheme_name": "योजना का नाम",
        "district": "जिला",
        "status": "लाभार्थी स्थिति",
        "eligible_status": "✓ पात्र",
        "why_eligible": "आप पात्र क्यों हैं",
        "meet_criteria": "आप निम्नलिखित पात्रता मानदंडों को पूरा करते हैं:",
        "overview": "योजना विवरण",
        "profile_summary": "आपकी प्रोफाइल सारांश",
        "income_bracket": "आय स्तर",
        "annual_income": "वार्षिक आय",
        "category": "श्रेणी",
        "occupation": "व्यवसाय",
        "land_holding": "भूमि होल्डिंग्स",
        "acres": "एकड़",
        "legal_rights": "आपके अधिकार और कानूनी सहायता",
        "if_denied_delayed": "यदि आपके लाभ अस्वीकार या विलंबित हों:",
        "rights_available": "आपके पास निम्नलिखित अधिकार और विकल्प हैं:",
        "appeal_process": "अपील प्रक्रिया",
        "appeal_desc": "अस्वीकार के 30 दिन के भीतर समीक्षा का अनुरोध करें",
        "ombudsman": "लोकपाल शिकायत",
        "ombudsman_desc": "राज्य/जिला लोकपाल को शिकायत दर्ज करें",
        "rti": "सूचना का अधिकार",
        "rti_desc": "निर्णय रिकॉर्ड प्राप्त करने के लिए RTI अधिनियम का उपयोग करें",
        "legal_aid": "कानूनी सहायता",
        "legal_aid_desc": "जिला कानूनी सेवा प्राधिकरण के माध्यम से मुफ्त कानूनी सहायता प्राप्त करें",
        "petition": "याचिका",
        "petition_desc": "जिला प्रशासनिक न्यायालय या उपभोक्ता न्यायालय में याचिका दाखिल करें",
        "lokayukta": "लोकायुक्त (भ्रष्टाचार विरोधी)",
        "lokayukta_desc": "भ्रष्टाचार या कदाचार की रिपोर्ट करें",
        "timeline_benefits": "लाभों के लिए समयसीमा:",
        "application_processing": "आवेदन प्रसंस्करण",
        "app_proc_time": "7-14 दिन",
        "approval": "अनुमोदन",
        "approval_time": "14-30 दिन",
        "first_disbursement": "पहली किश्त",
        "disburse_time": "अनुमोदन से 30-45 दिन",
        "ongoing": "चल रहे लाभ",
        "ongoing_time": "योजना शर्तों के अनुसार मासिक/त्रैमासिक",
        "required_documents": "आपको दाखिल/जमा करनी पड़ सकती हैं ऐसी दस्तावेजें:",
        "aadhaar_card": "मूल आधार कार्ड या UIDAI से आधार पत्र",
        "address_proof": "पता प्रमाण (बिजली बिल, पानी बिल, किराया समझौता)",
        "income_cert": "तहसील कार्यालय से आय प्रमाणपत्र",
        "land_records": "भूमि रिकॉर्ड (यदि लागू हो) - 7/12 अर्क",
        "bank_details": "बैंक खाता विवरण और पासबुक",
        "caste_cert": "जाति प्रमाणपत्र (यदि लागू हो)",
        "employment_proof": "रोजगार/व्यावसायिक प्रमाण",
        "family_details": "पारिवारिक विवरण और वैवाहिक स्थिति प्रमाणपत्र",
        "if_benefits_stopped": "यदि लाभ बिना किसी कारण के बंद हो जाएं:",
        "stop_step1": "7 दिन के भीतर विभाग से लिखित व्याख्या का अनुरोध करें",
        "stop_step2": "समर्थन दस्तावेजों के साथ औपचारिक अपील दाखिल करें",
        "stop_step3": "उच्च अधिकारी/जिला कलेक्टर को बढ़ाएं",
        "stop_step4": "आवश्यक होने पर न्यायालय में अंतरिम राहत के लिए आवेदन करें",
        "stop_step5": "समर्थन के लिए नागरिक स्वतंत्रता संगठनों से संपर्क करें",
        "cases_can_file": "आप दाखिल कर सकते हैं ऐसे मामले:",
        "case_writ": "प्रशासनिक याचिका",
        "case_writ_desc": "अवैध अस्वीकार के लिए उच्च न्यायालय में",
        "case_consumer": "उपभोक्ता न्यायालय मामला",
        "case_consumer_desc": "उपभोक्ता संरक्षण अधिनियम के अनुसार खराब सेवा के लिए",
        "case_criminal": "आपराधिक शिकायत",
        "case_criminal_desc": "अधिकारियों द्वारा रिकॉर्ड की जालसाजी/हेराफेरी के लिए",
        "case_civil": "नागरिक मुकदमा",
        "case_civil_desc": "गलत अस्वीकार के कारण नुकसान के लिए",
        "case_pil": "जनहित याचिका (PIL)",
        "case_pil_desc": "व्यवस्थागत विफलताओं के लिए",
        "important_notes": "महत्वपूर्ण नोट:",
        "note1": "यह प्रमाणपत्र वर्तमान मानदंडों के आधार पर आपकी पात्रता की पुष्टि करता है। योजनाएं बदल सकती हैं।",
        "note2": "इस प्रमाणपत्र को सुरक्षित रखें और भविष्य के संदर्भ के लिए पात्रता का प्रमाण रखें।",
        "note3": "कभी भी मूल दस्तावेज जमा करें; आधार संख्या को अनावश्यक रूप से साझा न करें।",
        "note4": "किसी भी धोखाधड़ी या भ्रष्टाचार की रिपोर्ट आधिकारिक शिकायत पोर्टल को करें।",
        "note5": "योजना-विशिष्ट प्रश्नों के लिए, जिला कल्याण कार्यालय से संपर्क करें।",
        "note6": "कानूनी सहायता सेवाएं जिला कानूनी सेवा प्राधिकरण के माध्यम से निःशुल्क उपलब्ध हैं।",
        "useful_contacts": "उपयोगी संपर्क",
        "district_office": "जिला कल्याण कार्यालय",
        "district_office_info": "अपने जिला कार्यालय की वेबसाइट देखें",
        "legal_services": "कानूनी सेवा प्राधिकरण",
        "legal_services_info": "1800-233-4415 (सहजय) पर कॉल करें",
        "lok_adalat": "लोक अदालत (जनता सभा)",
        "lok_adalat_info": "सरल विवादों के समाधान के लिए",
        "grievance": "शिकायत निवारण",
        "grievance_info": "prsindia.org या राज्य पोर्टल पर जाएं",
        "footer_line1": "यह अधिकार प्रमाणपत्र सरकारी कल्याण योजनाओं के लिए पात्रता का प्रमाण है।",
        "footer_line2": "प्रामाणिकता के लिए, जारी करने वाले प्राधिकरण के साथ प्रमाणपत्र ID सत्यापित करें।",
        "footer_line3": "द्वारा जेनरेट: अधिकार आइना - सरकारी कल्याण अधिकार मंच",
        "footer_line4": "तारीख: {generated_date} | प्रमाणपत्र ID: {cert_id}",
        "phase": "चरण",
        "duration": "अवधि",
        "action": "कार्य",
        "phase1_duration": "0-7 दिन",
        "phase1_action": "आवेदन स्वीकृति और सत्यापन",
        "phase2_duration": "7-30 दिन",
        "phase2_action": "दस्तावेज समीक्षा और पात्रता जांच",
        "phase3_duration": "30-45 दिन",
        "phase3_action": "लाभ वितरण प्राधिकरण",
    },
    "en": {
        "title": "⚖️ ADHIKAR CERTIFICATE",
        "subtitle": "Certificate of Eligibility & Rights",
        "subsubtitle": "Government Welfare Scheme Benefit",
        "cert_id": "Certificate ID",
        "generated": "Generated",
        "eligible": "ELIGIBILITY CONFIRMATION",
        "certifies": "This certifies that:",
        "citizen_id": "CITIZEN ID",
        "scheme_name": "SCHEME NAME",
        "district": "DISTRICT",
        "status": "BENEFICIARY STATUS",
        "eligible_status": "✓ ELIGIBLE",
        "why_eligible": "WHY YOU ARE ELIGIBLE",
        "meet_criteria": "You meet the following eligibility criteria:",
        "overview": "SCHEME OVERVIEW",
        "profile_summary": "YOUR PROFILE SUMMARY",
        "income_bracket": "Income Bracket",
        "annual_income": "Annual Income",
        "category": "Category",
        "occupation": "Occupation",
        "land_holding": "Land Holdings",
        "acres": "acres",
        "legal_rights": "YOUR RIGHTS & LEGAL RECOURSE",
        "if_denied_delayed": "If Your Benefits Are Denied or Delayed:",
        "rights_available": "You have the following rights and options:",
        "appeal_process": "Appeal Process",
        "appeal_desc": "Request a review within 30 days of denial",
        "ombudsman": "Ombudsman Complaint",
        "ombudsman_desc": "File a complaint with the State/District Ombudsman",
        "rti": "RTI Request",
        "rti_desc": "Use Right to Information Act to access decision records",
        "legal_aid": "Legal Aid",
        "legal_aid_desc": "Apply for free legal aid through District Legal Services Authority",
        "petition": "Petition",
        "petition_desc": "File a petition in District Administrative Court or Consumer Court",
        "lokayukta": "lokayukta (Anti-Corruption)",
        "lokayukta_desc": "Report corruption or misconduct",
        "timeline_benefits": "Timeline For Benefits:",
        "application_processing": "Application Processing",
        "app_proc_time": "7-14 days",
        "approval": "Approval",
        "approval_time": "14-30 days",
        "first_disbursement": "First Disbursement",
        "disburse_time": "30-45 days from approval",
        "ongoing": "Ongoing Benefits",
        "ongoing_time": "Monthly/Quarterly as per scheme terms",
        "required_documents": "Documents You May Need to File/Submit:",
        "aadhaar_card": "Original Aadhaar Card or Aadhaar Letter from UIDAI",
        "address_proof": "Address Proof (Electricity Bill, Water Bill, Lease Agreement)",
        "income_cert": "Income Certificate from Taluka Office",
        "land_records": "Land Records (if applicable) - 7/12 Extract",
        "bank_details": "Bank Account Details and Passbook",
        "caste_cert": "Caste Certificate (if applicable)",
        "employment_proof": "Employment/Occupation Proof",
        "family_details": "Family Details and Marital Status Certificate",
        "if_benefits_stopped": "If Benefits Are Stopped Without Reason:",
        "stop_step1": "Request written explanation from department within 7 days",
        "stop_step2": "File formal appeal with supporting documents",
        "stop_step3": "Escalate to Superior Officer/District Collector",
        "stop_step4": "Apply for interim relief in court if urgent",
        "stop_step5": "Contact Civil Liberties organizations for support",
        "cases_can_file": "Cases You Can File:",
        "case_writ": "Administrative Writ Petition",
        "case_writ_desc": "In High Court for illegal denial",
        "case_consumer": "Consumer Court Case",
        "case_consumer_desc": "For deficient service as per Consumer Protection Act",
        "case_criminal": "Criminal Complaint",
        "case_criminal_desc": "For forgery/falsification of records by officials",
        "case_civil": "Civil Suit",
        "case_civil_desc": "For damages due to wrongful denial",
        "case_pil": "Public Interest Litigation",
        "case_pil_desc": "For systematic failures",
        "important_notes": "IMPORTANT NOTES:",
        "note1": "This certificate confirms your eligibility based on current criteria. Schemes may change.",
        "note2": "Keep this certificate safe as proof of eligibility for future reference.",
        "note3": "Always submit original documents; never share Aadhaar number unnecessarily.",
        "note4": "Report any fraud or corruption to the official grievance portal.",
        "note5": "For scheme-specific queries, contact the District Welfare Office.",
        "note6": "Legal aid services are available free through District Legal Services Authority.",
        "useful_contacts": "USEFUL CONTACTS",
        "district_office": "District Welfare Office",
        "district_office_info": "Check your district office website",
        "legal_services": "Legal Services Authority",
        "legal_services_info": "Call 1800-233-4415 (Toll-free)",
        "lok_adalat": "Lok Adalat (Public Court)",
        "lok_adalat_info": "For simple disputes resolution",
        "grievance": "Grievance Redressal",
        "grievance_info": "Visit prsindia.org or state portal",
        "footer_line1": "This Adhikar Certificate is generated as proof of eligibility for government welfare schemes.",
        "footer_line2": "For authenticity, verify Certificate ID with the issuing authority.",
        "footer_line3": "Generated by: ADHIKAR - Government Welfare Rights Platform",
        "footer_line4": "Date: {generated_date} | Certificate ID: {cert_id}",
        "phase": "Phase",
        "duration": "Duration",
        "action": "Action",
        "phase1_duration": "0-7 days",
        "phase1_action": "Application acceptance and verification",
        "phase2_duration": "7-30 days",
        "phase2_action": "Document review and eligibility checking",
        "phase3_duration": "30-45 days",
        "phase3_action": "Benefit disbursement authorization",
    }
}

class AdhikarCertificateRequest(BaseModel):
    citizen_id: str
    scheme_name: str
    scheme_description: str
    language: Optional[str] = "en"
    eligibility_criteria: Dict[str, Any]
    citizen_profile: Dict[str, Any]


@app.post("/api/adhikar-certificate")
def generate_adhikar_certificate(payload: AdhikarCertificateRequest):
    """Generate a downloadable Adhikar Certificate with eligibility details."""
    try:
        cert_data = {
            "citizen_id": payload.citizen_id,
            "scheme_name": payload.scheme_name,
            "scheme_description": payload.scheme_description,
            "eligibility_criteria": payload.eligibility_criteria,
            "citizen_profile": payload.citizen_profile,
            "language": payload.language or "en",
            "generated_date": time.strftime("%Y-%m-%d"),
            "certificate_id": f"ADHIKAR-{payload.citizen_id}-{int(time.time())}",
        }
        
        # Build the certificate content with HTML formatting in the selected language
        html_content = build_adhikar_certificate_html(cert_data)
        
        print(f"✅ Generated Adhikar Certificate for {payload.citizen_id} - {payload.scheme_name}")
        
        return {
            "ok": True,
            "certificate_id": cert_data["certificate_id"],
            "html": html_content,
            "citizen_id": payload.citizen_id,
            "scheme_name": payload.scheme_name,
        }
    except Exception as e:
        print(f"❌ Certificate generation error: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


def build_adhikar_certificate_html(cert_data: Dict[str, Any]) -> str:
    """Build comprehensive Adhikar Certificate HTML with eligibility and legal recourse."""
    
    lang = cert_data.get("language", "en").lower()
    if lang not in CERTIFICATE_TRANSLATIONS:
        lang = "en"
    t = CERTIFICATE_TRANSLATIONS[lang]
    
    citizen = cert_data["citizen_profile"]
    eligibility = cert_data["eligibility_criteria"]
    
    # Build eligibility reasons
    reasons = []
    if eligibility.get("income_bracket"):
        reasons.append(f"Income Bracket: {eligibility['income_bracket']}")
    if eligibility.get("land_category"):
        reasons.append(f"Land Category: {eligibility['land_category']}")
    if eligibility.get("occupation_category"):
        reasons.append(f"Occupation: {eligibility['occupation_category']}")
    if citizen.get("has_daughter"):
        reasons.append("Has a daughter eligible for welfare support")
    if citizen.get("employment_days"):
        reasons.append(f"Employment Experience: {citizen['employment_days']} days")
    
    eligibility_text = "</li><li>".join(reasons) if reasons else "Meets scheme eligibility criteria"
    
    html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="UTF-8">
        <style>
            body {{ font-family: Arial, sans-serif; line-height: 1.6; color: #333; max-width: 900px; margin: 0 auto; }}
            .header {{ text-align: center; border-bottom: 3px solid #16a34a; padding: 20px 0; margin-bottom: 20px; }}
            .header h1 {{ color: #16a34a; margin: 0; font-size: 28px; }}
            .header p {{ margin: 5px 0; color: #666; }}
            .certificate-id {{ background: #f0fdf4; padding: 10px; text-align: center; margin-bottom: 20px; font-size: 12px; color: #666; border-radius: 4px; }}
            .section {{ margin: 20px 0; padding: 15px; background: #f9fafb; border-left: 4px solid #16a34a; border-radius: 4px; }}
            .section h2 {{ color: #16a34a; margin-top: 0; font-size: 18px; }}
            .section h3 {{ color: #1f2937; font-size: 15px; margin: 10px 0 5px 0; }}
            .info-grid {{ display: grid; grid-template-columns: 1fr 1fr; gap: 15px; margin: 10px 0; }}
            .info-item {{ padding: 10px; background: white; border-radius: 4px; }}
            .info-label {{ font-weight: bold; color: #16a34a; font-size: 13px; }}
            .info-value {{ margin-top: 3px; color: #333; }}
            ul {{ margin: 10px 0; padding-left: 20px; }}
            li {{ margin: 8px 0; }}
            .legal-section {{ background: #fef3c7; border-left: 4px solid #f59e0b; padding: 15px; border-radius: 4px; margin: 20px 0; }}
            .legal-section h2 {{ color: #d97706; margin-top: 0; }}
            .legal-section h3 {{ color: #b45309; margin: 15px 0 10px 0; font-size: 14px; }}
            .important {{ background: #fee2e2; border-left: 4px solid #dc2626; padding: 15px; border-radius: 4px; margin: 15px 0; }}
            .important h3 {{ color: #dc2626; margin-top: 0; }}
            .footer {{ margin-top: 30px; padding-top: 20px; border-top: 1px solid #ddd; text-align: center; color: #666; font-size: 12px; }}
            .signature-box {{ margin-top: 30px; display: grid; grid-template-columns: 1fr 1fr; gap: 30px; text-align: center; }}
            .signature-line {{ border-top: 2px solid #333; margin-top: 50px; }}
            table {{ width: 100%; border-collapse: collapse; margin: 10px 0; }}
            th, td {{ padding: 10px; text-align: left; border: 1px solid #ddd; }}
            th {{ background: #16a34a; color: white; }}
        </style>
    </head>
    <body>
        <div class="header">
            <h1>⚖️ {t['title']}</h1>
            <p>{t['subtitle']}</p>
            <p style="font-size: 12px; color: #999;">{t['subsubtitle']}</p>
        </div>
        
        <div class="certificate-id">
            {t['cert_id']}: {cert_data['certificate_id']} | {t['generated']}: {cert_data['generated_date']}
        </div>

        <div class="section">
            <h2>✅ {t['eligible']}</h2>
            <p><strong>{t['certifies']}</strong></p>
            <div class="info-grid">
                <div class="info-item">
                    <div class="info-label">{t['citizen_id']}</div>
                    <div class="info-value">{cert_data['citizen_id']}</div>
                </div>
                <div class="info-item">
                    <div class="info-label">{t['scheme_name']}</div>
                    <div class="info-value">{cert_data['scheme_name']}</div>
                </div>
                <div class="info-item">
                    <div class="info-label">{t['district']}</div>
                    <div class="info-value">{citizen.get('district', 'N/A')}</div>
                </div>
                <div class="info-item">
                    <div class="info-label">{t['status']}</div>
                    <div class="info-value" style="color: #16a34a; font-weight: bold;">{t['eligible_status']}</div>
                </div>
            </div>
        </div>

        <div class="section">
            <h2>📋 {t['why_eligible']}</h2>
            <p><strong>{t['meet_criteria']}</strong></p>
            <ul>
                <li>{eligibility_text}</li>
            </ul>
        </div>

        <div class="section">
            <h2>📄 {t['overview']}</h2>
            <p><strong>{cert_data['scheme_name']}</strong></p>
            <p>{cert_data['scheme_description']}</p>
        </div>

        <div class="section">
            <h2>💰 {t['profile_summary']}</h2>
            <table>
                <tr>
                    <th>Attribute</th>
                    <th>Value</th>
                </tr>
                <tr>
                    <td>{t['income_bracket']}</td>
                    <td>{citizen.get('income_bracket', 'N/A')}</td>
                </tr>
                <tr>
                    <td>{t['annual_income']}</td>
                    <td>₹{citizen.get('annual_income', 'N/A'):,}</td>
                </tr>
                <tr>
                    <td>{t['category']}</td>
                    <td>{citizen.get('category', 'N/A')}</td>
                </tr>
                <tr>
                    <td>{t['occupation']}</td>
                    <td>{citizen.get('occupation_category', 'N/A')}</td>
                </tr>
                <tr>
                    <td>{t['land_holding']}</td>
                    <td>{citizen.get('land_acres', 'N/A')} {t['acres']} ({citizen.get('land_category', 'N/A')})</td>
                </tr>
            </table>
        </div>

        <div class="legal-section">
            <h2>⚖️ {t['legal_rights']}</h2>
            
            <h3>{t['if_denied_delayed']}</h3>
            <p><strong>{t['rights_available']}</strong></p>
            <ul>
                <li><strong>{t['appeal_process']}:</strong> {t['appeal_desc']}</li>
                <li><strong>{t['ombudsman']}:</strong> {t['ombudsman_desc']}</li>
                <li><strong>{t['rti']}:</strong> {t['rti_desc']}</li>
                <li><strong>{t['legal_aid']}:</strong> {t['legal_aid_desc']}</li>
                <li><strong>{t['petition']}:</strong> {t['petition_desc']}</li>
                <li><strong>{t['lokayukta']}:</strong> {t['lokayukta_desc']}</li>
            </ul>

            <h3>{t['timeline_benefits']}</h3>
            <ul>
                <li><strong>{t['application_processing']}:</strong> {t['app_proc_time']}</li>
                <li><strong>{t['approval']}:</strong> {t['approval_time']}</li>
                <li><strong>{t['first_disbursement']}:</strong> {t['disburse_time']}</li>
                <li><strong>{t['ongoing']}:</strong> {t['ongoing_time']}</li>
            </ul>

            <h3>{t['required_documents']}</h3>
            <ul>
                <li>{t['aadhaar_card']}</li>
                <li>{t['address_proof']}</li>
                <li>{t['income_cert']}</li>
                <li>{t['land_records']}</li>
                <li>{t['bank_details']}</li>
                <li>{t['caste_cert']}</li>
                <li>{t['employment_proof']}</li>
                <li>{t['family_details']}</li>
            </ul>

            <h3>{t['if_benefits_stopped']}</h3>
            <ul>
                <li>{t['stop_step1']}</li>
                <li>{t['stop_step2']}</li>
                <li>{t['stop_step3']}</li>
                <li>{t['stop_step4']}</li>
                <li>{t['stop_step5']}</li>
            </ul>

            <h3>{t['cases_can_file']}</h3>
            <ul>
                <li><strong>{t['case_writ']}:</strong> {t['case_writ_desc']}</li>
                <li><strong>{t['case_consumer']}:</strong> {t['case_consumer_desc']}</li>
                <li><strong>{t['case_criminal']}:</strong> {t['case_criminal_desc']}</li>
                <li><strong>{t['case_civil']}:</strong> {t['case_civil_desc']}</li>
                <li><strong>{t['case_pil']}:</strong> {t['case_pil_desc']}</li>
            </ul>
        </div>

        <div class="important">
            <h3>⚠️ {t['important_notes']}</h3>
            <ul>
                <li>{t['note1']}</li>
                <li>{t['note2']}</li>
                <li>{t['note3']}</li>
                <li>{t['note4']}</li>
                <li>{t['note5']}</li>
                <li>{t['note6']}</li>
            </ul>
        </div>

        <div class="section">
            <h2>📞 {t['useful_contacts']}</h2>
            <ul>
                <li><strong>{t['district_office']}:</strong> {t['district_office_info']}</li>
                <li><strong>{t['legal_services']}:</strong> {t['legal_services_info']}</li>
                <li><strong>{t['lok_adalat']}:</strong> {t['lok_adalat_info']}</li>
                <li><strong>{t['grievance']}:</strong> {t['grievance_info']}</li>
            </ul>
        </div>

        <div class="footer">
            <p>{t['footer_line1']}</p>
            <p>{t['footer_line2']}</p>
            <p>{t['footer_line3']}</p>
            <p>Date: {cert_data['generated_date']} | Certificate ID: {cert_data['certificate_id']}</p>
        </div>
    </body>
    </html>
    """
    
    return html