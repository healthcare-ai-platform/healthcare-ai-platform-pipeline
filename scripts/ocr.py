import io
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

from common.logger import get_logger
from common.s3 import S3_BUCKET, download_file, upload_bytes

log = get_logger(__name__)

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


# ── Public entry point ────────────────────────────────────────────────────────

def extract_pdf(s3_key: str, document_id: str, tenant_id: str, report_type: str) -> tuple[str, str]:
    """
    Read a PDF from S3 bronze, extract structured patient + report + test result
    data using a LangChain tool-calling agent, then write two partitioned Parquet
    files to S3 silver:

      processed/ocr_extractions/tenant=.../year=.../month=.../day=.../<doc_id>.parquet
      processed/ocr_results/tenant=.../year=.../month=.../day=.../<doc_id>.parquet

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

    now          = datetime.now(tz=timezone.utc)
    extracted_at = now.isoformat()
    partition    = (
        f"tenant={tenant_id}"
        f"/year={now.year}/month={now.month:02d}/day={now.day:02d}"
    )

    # 3. Write summary Parquet — one row per document
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

    # 4. Write results Parquet — one row per test result
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
        "OCR complete for %s — %d test results → s3://%s/{%s, %s}",
        document_id, len(state.results), S3_BUCKET, summary_key, results_key,
    )
    return summary_key, results_key
