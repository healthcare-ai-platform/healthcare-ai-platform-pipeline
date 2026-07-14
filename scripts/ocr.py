import io
import json
import os
from datetime import datetime, timezone
from typing import Optional

import pandas as pd
import pdfplumber
from langchain.agents import AgentExecutor, create_tool_calling_agent
from langchain_anthropic import ChatAnthropic
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.tools import tool
from pydantic import Field

from common.db import get_connection
from common.logger import get_logger
from common.s3 import S3_BUCKET, download_file, upload_bytes

log = get_logger(__name__)

# Result-level flags the `report_results` CHECK constraint accepts — anything
# else gets stored as NULL rather than failing the whole insert.
_VALID_FLAGS = {"normal", "high", "low", "critical", "borderline"}

SILVER_PREFIX = os.getenv("SILVER_PREFIX", "processed")

# Skip the Claude tool-calling agent and use canned data instead — for testing
# the rest of the pipeline (S3 write, Snowpipe, warehouse load) without
# spending Anthropic credits. Flip back to "false" once billing is sorted.
OCR_MOCK_MODE = os.getenv("OCR_MOCK_MODE", "false").lower() == "true"


# ── Mutable state the agent populates via tool calls ─────────────────────────

class _ExtractionState:
    def __init__(self):
        self.patient: dict  = {}
        self.report:  dict  = {}
        self.results: list  = []


def _dummy_extraction_state() -> _ExtractionState:
    """Canned stand-in for the Claude agent's output — see OCR_MOCK_MODE."""
    state = _ExtractionState()
    state.patient = {
        "patient_name":        "Test Patient",
        "patient_external_id": "MOCK-0001",
        "patient_dob":         "1990-01-01",
        "patient_gender":      "other",
    }
    state.report = {
        "report_date":           datetime.now(tz=timezone.utc).date().isoformat(),
        "doctor":                "Dr. Mock",
        "facility":              "Mock Facility",
        "extraction_confidence": 0.0,
    }
    state.results = [
        {
            "test_name":       "Mock Test",
            "value":           "0",
            "unit":            "n/a",
            "reference_range": "n/a",
            "flag":            "normal",
        },
    ]
    return state


# ── Agent factory ─────────────────────────────────────────────────────────────

def _build_agent(state: _ExtractionState) -> AgentExecutor:
    llm = ChatAnthropic(
        model="claude-haiku-4-5-20251001",
        api_key=os.environ["ANTHROPIC_API_KEY"],
        temperature=0,
        max_tokens=4096,
    )

    @tool
    def save_patient_info(
        name:        str           = Field(description="Patient full name"),
        external_id: Optional[str] = Field(None, description="MRN or patient ID"),
        dob:         Optional[str] = Field(None, description="Date of birth YYYY-MM-DD"),
        gender:      Optional[str] = Field(None, description="male | female | other"),
    ) -> str:
        """Save extracted patient demographics. Call exactly once."""
        state.patient = {
            "patient_name":        name,
            "patient_external_id": external_id,
            "patient_dob":         dob,
            "patient_gender":      gender,
        }
        return "Patient info saved."

    @tool
    def save_report_metadata(
        report_date: Optional[str]   = Field(None, description="Report date YYYY-MM-DD"),
        doctor:      Optional[str]   = Field(None, description="Ordering physician full name"),
        facility:    Optional[str]   = Field(None, description="Hospital or laboratory name"),
        confidence:  float           = Field(description="Extraction confidence 0.0–1.0"),
    ) -> str:
        """Save extracted report metadata. Call exactly once."""
        state.report = {
            "report_date":           report_date,
            "doctor":                doctor,
            "facility":              facility,
            "extraction_confidence": confidence,
        }
        return "Report metadata saved."

    @tool
    def save_test_result(
        test_name:       str           = Field(description="Lab test or measurement name"),
        value:           str           = Field(description="Observed value as a string"),
        unit:            Optional[str] = Field(None, description="Unit of measurement"),
        reference_range: Optional[str] = Field(None, description="Normal range e.g. 70–100"),
        flag:            Optional[str] = Field(None, description="critical | high | low | borderline | normal"),
    ) -> str:
        """Save one test result. Call once per lab value found in the document."""
        state.results.append({
            "test_name":       test_name,
            "value":           value,
            "unit":            unit,
            "reference_range": reference_range,
            "flag":            flag,
        })
        return f"Result '{test_name}' saved."

    tools = [save_patient_info, save_report_metadata, save_test_result]

    prompt = ChatPromptTemplate.from_messages([
        (
            "system",
            (
                "You are a medical document extraction agent.\n"
                "Read the document carefully and extract all available information:\n"
                "1. Call save_patient_info ONCE with the patient's demographics.\n"
                "2. Call save_report_metadata ONCE with the report details and your confidence (0–1).\n"
                "3. Call save_test_result ONCE for EVERY lab test or measurement you find.\n"
                "Use null for any field you cannot find. Do not invent values."
            ),
        ),
        ("human", "{input}"),
        ("placeholder", "{agent_scratchpad}"),
    ])

    agent = create_tool_calling_agent(llm, tools, prompt)
    return AgentExecutor(agent=agent, tools=tools, verbose=False, max_iterations=30)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _write_parquet(df: pd.DataFrame, key: str) -> None:
    buf = io.BytesIO()
    df.to_parquet(buf, index=False, engine="pyarrow", compression="snappy")
    buf.seek(0)
    upload_bytes(key, buf.read())


def _write_silver_parquets(document_id: str, tenant_id: str, report_type: str, state: _ExtractionState) -> tuple[str, str]:
    """
    Write the two partitioned Parquet files shared by every extraction source
    (OCR'd PDFs and pre-structured JSON uploads alike):

      processed/ocr_extractions/tenant=.../year=.../month=.../day=.../<doc_id>.parquet
      processed/ocr_results/tenant=.../year=.../month=.../day=.../<doc_id>.parquet

    Returns (summary_key, results_key).
    """
    now          = datetime.now(tz=timezone.utc)
    extracted_at = now.isoformat()
    partition    = (
        f"tenant={tenant_id}"
        f"/year={now.year}/month={now.month:02d}/day={now.day:02d}"
    )

    summary_df = pd.DataFrame([{
        "document_id":           document_id,
        "tenant_id":             tenant_id,
        "report_type":           report_type,
        **state.patient,
        **state.report,
        "extraction_status":     "completed",
        "extracted_at":          extracted_at,
    }])
    summary_key = f"{SILVER_PREFIX}/ocr_extractions/{partition}/{document_id}.parquet"
    _write_parquet(summary_df, summary_key)

    results_rows = [
        {"document_id": document_id, "tenant_id": tenant_id, **r, "extracted_at": extracted_at}
        for r in state.results
    ]
    results_df = pd.DataFrame(results_rows) if results_rows else pd.DataFrame(columns=[
        "document_id", "tenant_id", "test_name", "value",
        "unit", "reference_range", "flag", "extracted_at",
    ])
    results_key = f"{SILVER_PREFIX}/ocr_results/{partition}/{document_id}.parquet"
    _write_parquet(results_df, results_key)

    log.info(
        "Wrote silver Parquets for %s — %d test results → s3://%s/{%s, %s}",
        document_id, len(state.results), S3_BUCKET, summary_key, results_key,
    )
    return summary_key, results_key


def _parse_numeric(value) -> Optional[float]:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def write_operational_records(
    document_id: str, tenant_id: str, facility_id: str, report_type: str,
    summary_key: str, state: _ExtractionState,
) -> None:
    """
    Persist extracted patient/report/results into the operational Postgres
    tables the product UI actually reads (patients, reports, report_results) —
    without this, extraction only ever reaches Snowflake analytics and never
    shows up on the Patients page.
    """
    patient = state.patient
    report  = state.report

    name = patient.get("patient_name")
    dob  = patient.get("patient_dob")
    if not name or not dob:
        raise ValueError(f"Extraction incomplete for {document_id} — missing patient name/dob")

    external_id = patient.get("patient_external_id") or document_id
    gender      = patient.get("patient_gender") or "unknown"

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO patients (tenant_id, external_id, name, dob, gender)
                VALUES (%(tenant_id)s, %(external_id)s, %(name)s, %(dob)s, %(gender)s)
                ON CONFLICT (tenant_id, external_id) DO UPDATE
                    SET name = EXCLUDED.name, updated_at = NOW()
                RETURNING patient_id
                """,
                {"tenant_id": tenant_id, "external_id": external_id, "name": name, "dob": dob, "gender": gender},
            )
            patient_id = cur.fetchone()[0]

            cur.execute(
                """
                INSERT INTO reports (
                    document_id, patient_id, facility_id, report_type, report_date,
                    doctor, extraction_status, extraction_confidence, s3_extracted_path
                ) VALUES (
                    %(document_id)s, %(patient_id)s, %(facility_id)s, %(report_type)s, %(report_date)s,
                    %(doctor)s, 'extracted', %(confidence)s, %(s3_path)s
                )
                ON CONFLICT (document_id) DO UPDATE
                    SET extraction_status     = EXCLUDED.extraction_status,
                        extraction_confidence = EXCLUDED.extraction_confidence,
                        updated_at             = NOW()
                RETURNING report_id
                """,
                {
                    "document_id": document_id,
                    "patient_id":  patient_id,
                    "facility_id": facility_id,
                    "report_type": report_type,
                    "report_date": report.get("report_date"),
                    "doctor":      report.get("doctor"),
                    "confidence":  report.get("extraction_confidence"),
                    "s3_path":     summary_key,
                },
            )
            report_id = cur.fetchone()[0]

            # Re-processing the same document replaces its results rather than
            # appending duplicates.
            cur.execute("DELETE FROM report_results WHERE report_id = %s", (report_id,))
            for r in state.results:
                flag = r.get("flag") if r.get("flag") in _VALID_FLAGS else None
                cur.execute(
                    """
                    INSERT INTO report_results (report_id, test_name, value, unit, reference_range, flag)
                    VALUES (%(report_id)s, %(test_name)s, %(value)s, %(unit)s, %(reference_range)s, %(flag)s)
                    """,
                    {
                        "report_id":       report_id,
                        "test_name":       r.get("test_name") or "Unknown",
                        "value":           _parse_numeric(r.get("value")),
                        "unit":            r.get("unit"),
                        "reference_range": r.get("reference_range"),
                        "flag":            flag,
                    },
                )

            cur.execute(
                "UPDATE documents SET patient_id = %(patient_id)s WHERE document_id = %(document_id)s",
                {"patient_id": patient_id, "document_id": document_id},
            )

    log.info("[%s] Operational records written — patient_id=%s", document_id, patient_id)


def _state_from_structured_json(raw_bytes: bytes) -> _ExtractionState:
    """
    Build an _ExtractionState from an already-structured PatientRecord JSON
    payload (see upload.py's /upload/json endpoint) — the data is already
    clean, so this skips OCR/Claude entirely.
    """
    payload = json.loads(raw_bytes)
    state = _ExtractionState()
    state.patient = {
        "patient_name":        payload["patient"]["name"],
        "patient_external_id": payload["patient"].get("external_id"),
        "patient_dob":         payload["patient"].get("dob"),
        "patient_gender":      payload["patient"].get("gender"),
    }
    state.report = {
        "report_date":           payload["report"].get("date"),
        "doctor":                payload["report"].get("doctor"),
        "facility":              payload["report"].get("facility"),
        "extraction_confidence": payload.get("extraction_confidence", 1.0),
    }
    state.results = [
        {
            "test_name":       r["test_name"],
            "value":           r.get("value"),
            "unit":            r.get("unit"),
            "reference_range": r.get("reference_range"),
            "flag":            r.get("flag"),
        }
        for r in payload.get("results", [])
    ]
    return state


# ── Public entry points ────────────────────────────────────────────────────────

def extract_pdf(
    s3_key: str, document_id: str, tenant_id: str, facility_id: str, report_type: str,
) -> tuple[str, str]:
    """
    Read a PDF from S3 bronze, extract structured patient + report + test result
    data using a LangChain tool-calling agent, write the two silver Parquets,
    and persist the same data into the operational Postgres tables.

    Returns (summary_key, results_key).
    """
    # 1. Pull PDF from S3 and extract raw text page-by-page
    raw_bytes = download_file(s3_key)
    pages = []
    with pdfplumber.open(io.BytesIO(raw_bytes)) as pdf:
        for page in pdf.pages:
            text = page.extract_text() or ""
            pages.append(text.strip())

    full_text = "\n\n".join(pages)
    if not full_text.strip() and not OCR_MOCK_MODE:
        raise ValueError(f"No text could be extracted from {s3_key}")

    # 2. Run the LangChain extraction agent — or skip it in OCR_MOCK_MODE
    if OCR_MOCK_MODE:
        log.warning("[%s] OCR_MOCK_MODE enabled — using canned data instead of calling Claude", document_id)
        state = _dummy_extraction_state()
    else:
        state = _ExtractionState()
        agent = _build_agent(state)
        agent.invoke({"input": full_text[:14000]})  # stay within token budget

    summary_key, results_key = _write_silver_parquets(document_id, tenant_id, report_type, state)
    write_operational_records(document_id, tenant_id, facility_id, report_type, summary_key, state)
    return summary_key, results_key


def extract_structured_json(
    s3_key: str, document_id: str, tenant_id: str, facility_id: str, report_type: str,
) -> tuple[str, str]:
    """
    Read an already-structured PatientRecord JSON from S3 bronze (uploaded via
    /upload/json — no OCR needed since the data is already clean), write the
    same two silver Parquets as extract_pdf(), and persist into Postgres.

    Returns (summary_key, results_key).
    """
    raw_bytes = download_file(s3_key)
    state = _state_from_structured_json(raw_bytes)

    summary_key, results_key = _write_silver_parquets(document_id, tenant_id, report_type, state)
    write_operational_records(document_id, tenant_id, facility_id, report_type, summary_key, state)
    return summary_key, results_key
