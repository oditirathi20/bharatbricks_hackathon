import json
import os
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

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


@app.post("/check-eligibility", response_model=CheckEligibilityResponse)
def check_eligibility(payload: CheckEligibilityRequest) -> CheckEligibilityResponse:
    profile = {
        "income": float(payload.income or 0),
        "occupation": _normalized_occupation(payload.occupation),
        "land_acres": float(payload.land_acres or 0),
        "category": _normalized_category(payload.category),
    }

    try:
        schemes = _fetch_schemes_for_matching()
        eligible_schemes = [
            EligibilityScheme(
                scheme_name=str(scheme.get("scheme_name") or ""),
                benefit=str(scheme.get("benefit") or ""),
            )
            for scheme in schemes
            if _is_eligible(profile, scheme)
        ]

        return CheckEligibilityResponse(
            input_profile=profile,
            eligible_schemes=eligible_schemes,
            total_eligible=len(eligible_schemes),
        )
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
            income,
            land_acres,
            category,
            has_daughter,
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
