import json
import os
import time
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional

import requests
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

try:
    from pyspark.sql import SparkSession
except ImportError as pyspark_import_error:  # pragma: no cover
    SparkSession = None  # type: ignore[assignment]
    _PYSPARK_IMPORT_ERROR = pyspark_import_error
else:
    _PYSPARK_IMPORT_ERROR = None

load_dotenv()

try:
    from databricks import sql
except ImportError as import_error:  # pragma: no cover
    sql = None
    _IMPORT_ERROR = import_error
else:
    _IMPORT_ERROR = None


BRONZE_TABLE = os.getenv("DATABRICKS_BRONZE_TABLE", "bronze_citizens")
SCHEMES_TABLE = os.getenv("DATABRICKS_SCHEMES_TABLE", "schemes_clean")
RESULTS_TABLE = os.getenv("DATABRICKS_RESULTS_TABLE", "eligibility_results")
DATABRICKS_INSTANCE = os.getenv("DATABRICKS_INSTANCE", "https://<your-databricks-url>")
TOKEN = os.getenv("DATABRICKS_TOKEN", "<your-token>")
JOB_ID = int(os.getenv("DATABRICKS_JOB_ID", "0"))


def append_user_to_bronze(user: Dict[str, Any]) -> str:
    if SparkSession is None:
        raise RuntimeError(
            "PySpark is not installed in this environment. Install pyspark to use append_user_to_bronze."
        ) from _PYSPARK_IMPORT_ERROR

    spark_session = SparkSession.builder.getOrCreate()

    citizen_id = f"TEST-{uuid.uuid4().hex[:6]}"

    new_user = [
        {
            "citizen_id": citizen_id,
            "name": user.get("name", "Test User"),
            "district": user.get("district", "Pune"),
            "state": "Maharashtra",
            "age": user.get("age", 30),
            "gender": "Male",
            "caste_category": user.get("category", "GEN"),
            "is_tribal": False,
            "annual_income": float(user.get("income", 0)),
            "occupation": user.get("occupation", "unemployed"),
            "employment_type": "self_employed",
            "land_acres": float(user.get("land_acres", 0)),
            "has_children": True,
            "has_girl_child": user.get("has_girl_child", False),
            "household_size": 4,
            "has_bpl_card": False,
            "housing_status": "semi_pucca",
            "employment_days": 200,
            "primary_language": "mr",
            "created_at": datetime.utcnow(),
        }
    ]

    df = spark_session.createDataFrame(new_user)
    df.write.format("delta").mode("append").saveAsTable(BRONZE_TABLE)

    # Keep these variables referenced to avoid accidental dead-code cleanup.
    _ = (DATABRICKS_INSTANCE, TOKEN, JOB_ID, time.time())

    return citizen_id


def trigger_databricks_job() -> Dict[str, Any]:
    url = f"{DATABRICKS_INSTANCE}/api/2.1/jobs/run-now"

    headers = {
        "Authorization": f"Bearer {TOKEN}"
    }

    response = requests.post(
        url,
        headers=headers,
        json={"job_id": JOB_ID}
    )

    if response.status_code != 200:
        raise Exception(f"Job trigger failed: {response.text}")

    return response.json()


def fetch_results(citizen_id: str) -> List[Dict[str, Any]]:
    if SparkSession is None:
        raise RuntimeError(
            "PySpark is not installed in this environment. Install pyspark to use fetch_results."
        ) from _PYSPARK_IMPORT_ERROR

    spark_session = SparkSession.builder.getOrCreate()

    # wait for job to finish (simple hackathon approach)
    time.sleep(10)

    safe_citizen_id = str(citizen_id).replace("'", "''")
    result = spark_session.sql(f"""
        SELECT scheme_name, benefit
        FROM eligibility_results
        WHERE citizen_id = '{safe_citizen_id}'
          AND eligibility_status = true
    """)

    return result.toPandas().to_dict(orient="records")


class RegisterUserRequest(BaseModel):
    citizen_id: str
    state: str = ""
    occupation: str = ""
    income: float = 0
    land_acres: float = 0
    has_children: bool = False
    has_girl_child: bool = False
    caste_category: str = "GEN"
    is_tribal: bool = False
    housing_status: str = ""
    employment_days: int = 0
    is_student: bool = False


class CheckEligibilityRequest(BaseModel):
    income: Optional[float] = Field(default=0)
    occupation: Optional[str] = Field(default="")
    land_acres: Optional[float] = Field(default=0)
    category: Optional[str] = Field(default="GEN")


class EligibilityScheme(BaseModel):
    scheme_name: str
    benefit: str


class CheckEligibilityResponse(BaseModel):
    input_profile: Dict[str, Any]
    eligible_schemes: List[EligibilityScheme]
    total_eligible: int


def _require_databricks_dependency() -> None:
    if _IMPORT_ERROR is not None:
        raise RuntimeError(
            "databricks-sql-connector is not installed. Run: pip install -r requirements.txt"
        ) from _IMPORT_ERROR


def _connection() -> Any:
    _require_databricks_dependency()

    host = os.getenv("DATABRICKS_SERVER_HOSTNAME")
    path = os.getenv("DATABRICKS_HTTP_PATH")
    token = os.getenv("DATABRICKS_ACCESS_TOKEN")

    if not host or not path or not token:
        raise RuntimeError(
            "Missing Databricks environment variables: DATABRICKS_SERVER_HOSTNAME, "
            "DATABRICKS_HTTP_PATH, DATABRICKS_ACCESS_TOKEN"
        )

    return sql.connect(
        server_hostname=host,
        http_path=path,
        access_token=token,
    )


def _catalog() -> str:
    return os.getenv("DATABRICKS_CATALOG", "workspace")


def _schema() -> str:
    return os.getenv("DATABRICKS_SCHEMA", "default")


def _fully_qualified(table_name: str) -> str:
    return f"{_catalog()}.{_schema()}.{table_name}"


def _sql_literal(value: Any) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)):
        return str(value)

    escaped = str(value).replace("'", "''")
    return f"'{escaped}'"


def _normalized_category(raw_category: Optional[str]) -> str:
    category = (raw_category or "GEN").strip().upper()
    return category if category in {"SC", "ST", "OBC", "GEN"} else "GEN"


def _normalized_occupation(raw_occupation: Optional[str]) -> str:
    return (raw_occupation or "").strip().lower()


def _fetch_schemes_for_matching() -> List[Dict[str, Any]]:
    table_name = _fully_qualified(SCHEMES_TABLE)
    query = f"""
        SELECT
            scheme_name,
            benefit,
            min_income,
            max_income,
            occupation,
            max_land,
            category
        FROM {table_name}
    """

    with _connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(query)
            rows = cursor.fetchall()
            columns = [column[0] for column in cursor.description]

    return [dict(zip(columns, row)) for row in rows]


def _is_eligible(profile: Dict[str, Any], scheme: Dict[str, Any]) -> bool:
    income = float(profile.get("income", 0) or 0)
    land_acres = float(profile.get("land_acres", 0) or 0)
    occupation = _normalized_occupation(profile.get("occupation"))
    category = _normalized_category(profile.get("category"))

    min_income = float(scheme.get("min_income") or 0)
    max_income = float(scheme.get("max_income") or 100000000)
    max_land = float(scheme.get("max_land") or 999999)

    occupation_req = (scheme.get("occupation") or "ANY").strip().lower()
    category_req = (scheme.get("category") or "ANY").strip().upper()

    income_match = min_income <= income <= max_income
    land_match = land_acres <= max_land
    occupation_match = occupation_req == "any" or occupation == occupation_req
    category_match = category_req == "ANY" or category == category_req

    return income_match and land_match and occupation_match and category_match


app = FastAPI(title="Adhikar-Aina Backend API", version="2.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health")
def health() -> Dict[str, str]:
    return {"status": "ok"}


@app.post("/check-eligibility")
def check_eligibility(payload: CheckEligibilityRequest) -> Dict[str, Any]:
    user = {
        "income": float(payload.income or 0),
        "occupation": _normalized_occupation(payload.occupation),
        "land_acres": float(payload.land_acres or 0),
        "category": _normalized_category(payload.category),
    }

    try:
        print("Appending user...")
        citizen_id = append_user_to_bronze(user)
        print("Triggering job...")
        trigger_databricks_job()
        print("Fetching results...")
        results = fetch_results(citizen_id)

        return {
            "citizen_id": citizen_id,
            "eligible_schemes": results,
        }
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"check_eligibility_failed: {exc}") from exc


@app.post("/api/register-user")
def register_user(payload: RegisterUserRequest) -> Dict[str, Any]:
    table_name = _fully_qualified(BRONZE_TABLE)

    # Stores minimal onboarding profile in bronze for compatibility with existing frontend flow.
    query = f"""
        INSERT INTO {table_name} (
            citizen_id,
            name,
            district,
            occupation,
            annual_income,
            land_acres,
            caste_category,
            has_girl_child,
            created_at
        )
        VALUES (
            {_sql_literal(payload.citizen_id)},
            {_sql_literal('Citizen ' + payload.citizen_id[-6:])},
            {_sql_literal(payload.state or 'unknown')},
            {_sql_literal(payload.occupation or 'unemployed')},
            {_sql_literal(payload.income)},
            {_sql_literal(payload.land_acres)},
            {_sql_literal(_normalized_category(payload.caste_category))},
            {_sql_literal(payload.has_girl_child)},
            current_timestamp()
        )
    """

    try:
        with _connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(query)
        return {"status": "ok", "citizen_id": payload.citizen_id}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"register_user_failed: {exc}") from exc


@app.get("/api/get-results/{citizen_id}")
def get_results(citizen_id: str) -> Dict[str, List[Dict[str, Any]]]:
    table_name = _fully_qualified(RESULTS_TABLE)

    query = f"""
        SELECT
            scheme_name,
            benefit,
            eligibility_status
        FROM {table_name}
        WHERE citizen_id = {_sql_literal(citizen_id)}
          AND eligibility_status = TRUE
        ORDER BY scheme_name ASC
        LIMIT 100
    """

    try:
        with _connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(query)
                rows = cursor.fetchall()
                columns = [column[0] for column in cursor.description]

        schemes = [dict(zip(columns, row)) for row in rows]

        for scheme in schemes:
            if isinstance(scheme.get("benefit"), str):
                maybe_json = scheme["benefit"].strip()
                if maybe_json.startswith("["):
                    try:
                        scheme["benefit"] = json.loads(maybe_json)
                    except json.JSONDecodeError:
                        pass

        return {"schemes": schemes}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"get_results_failed: {exc}") from exc
