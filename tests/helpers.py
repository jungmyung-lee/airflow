from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
STAGE1_DIR = ROOT / "stage1" / "customer_care_emails"
STAGE2_DIR = ROOT / "stage2" / "customer_care_emails"
REAL_CSV = STAGE1_DIR / "customer_care_emails.csv"


def good_row(**overrides) -> dict:
    """One valid row (all values are text, like read_csv_as_text produces)."""
    row = {
        "subject": "Order delayed",
        "sender": "customer@example.com",
        "receiver": "support@example.com",
        "timestamp": "2023-10-26 10:02:34+00:00",
        "message_body": "Where is my order? It was due last week.",
        "thread_id": "thread-1",
        "email_types": "['inquiry']",
        "email_status": "ongoing",
        "email_criticality": "high",
        "product_types": "['laptop']",
        "agent_effectivity": "high",
        "agent_efficiency": "medium",
        "customer_satisfaction": "0.5",
    }
    row.update(overrides)
    return row


def frame(*rows: dict) -> pd.DataFrame:
    return pd.DataFrame(list(rows) or [good_row()], dtype=str)
