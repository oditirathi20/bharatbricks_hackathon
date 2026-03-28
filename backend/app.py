import json
import os
from typing import Any, Dict, List

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from dotenv import load_dotenv

load_dotenv()

try:
    from databricks import sql
except ImportError as import_error:  # pragma: no cover
    sql = None
    _IMPORT_ERROR = import_error
else:
    _IMPORT_ERROR = None


class RegisterUserRequest(BaseModel):
    citizen_id: str
    state: str = ""
    occupation: str = ""
    income: float = 0
    land_acres: float = 0
    has_children: bool = False
    has_girl_child: bool = False
    caste_category: str = "General"
    is_tribal: bool = False
    housing_status: str = ""
    employment_days: int = 0
    is_student: bool = False


def _sql_literal(value: Any) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)):
        return str(value)

    escaped = str(value).replace("'", "''")
    return f"'{escaped}'"


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


app = FastAPI(title="Adhikar-Aina Backend API", version="1.0.0")

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


@app.post("/api/register-user")
def register_user(payload: RegisterUserRequest) -> Dict[str, Any]:
    table_name = f"{_catalog()}.{_schema()}.aa_citizens_bronze"

    query = f"""
        INSERT INTO {table_name} (
            citizen_id,
            state,
            occupation,
            income,
            land_acres,
            has_children,
            has_girl_child,
            caste_category,
            is_tribal,
            housing_status,
            employment_days,
            is_student,
            created_at,
            updated_at
        )
        VALUES (
            {_sql_literal(payload.citizen_id)},
            {_sql_literal(payload.state)},
            {_sql_literal(payload.occupation)},
            {_sql_literal(payload.income)},
            {_sql_literal(payload.land_acres)},
            {_sql_literal(payload.has_children)},
            {_sql_literal(payload.has_girl_child)},
            {_sql_literal(payload.caste_category)},
            {_sql_literal(payload.is_tribal)},
            {_sql_literal(payload.housing_status)},
            {_sql_literal(payload.employment_days)},
            {_sql_literal(payload.is_student)},
            current_timestamp(),
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
    table_name = f"{_catalog()}.{_schema()}.aa_eligibility_results"

    query = f"""
        SELECT
            scheme_name,
            benefit,
            required_docs
        FROM {table_name}
        WHERE citizen_id = {_sql_literal(citizen_id)}
        ORDER BY matched_at DESC
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
            required_docs = scheme.get("required_docs")
            if isinstance(required_docs, str) and required_docs.strip().startswith("["):
                try:
                    scheme["required_docs"] = json.loads(required_docs)
                except json.JSONDecodeError:
                    pass

        return {"schemes": schemes}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"get_results_failed: {exc}") from exc
