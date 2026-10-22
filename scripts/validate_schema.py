import pandas as pd
import yaml


CSV_PATH = "stage1/customer_care_emails/customer_care_emails.csv"
SCHEMA_PATH = "stage1/customer_care_emails/schema.yaml"


def validate_schema():
    df = pd.read_csv(CSV_PATH)

    with open(SCHEMA_PATH, "r") as f:
        schema = yaml.safe_load(f)

    expected_columns = [c["name"] for c in schema["columns"]]
    actual_columns = list(df.columns)

    # 1. Column validation
    if expected_columns != actual_columns:
        raise ValueError(
            f"Column mismatch\n"
            f"Expected: {expected_columns}\n"
            f"Actual: {actual_columns}"
        )

    # 2. Column rules
    for col in schema["columns"]:
        name = col["name"]

        if not col.get("nullable", True):
            if df[name].isna().any():
                raise ValueError(
                    f"{name}: NULL values found"
                )

        if col["type"] == "timestamp":
            pd.to_datetime(
                df[name],
                format="mixed",
                utc=True,
                errors="raise",
            )

        elif col["type"] == "float":
            values = pd.to_numeric(
                df[name],
                errors="raise",
            )

            if "minimum" in col:
                if (values < col["minimum"]).any():
                    raise ValueError(
                        f"{name}: value below minimum"
                    )

            if "maximum" in col:
                if (values > col["maximum"]).any():
                    raise ValueError(
                        f"{name}: value above maximum"
                    )

        if "allowed_values" in col:
            invalid = (
                set(df[name].dropna())
                - set(col["allowed_values"])
            )

            if invalid:
                raise ValueError(
                    f"{name}: invalid values {invalid}"
                )

    # 3. Primary-key uniqueness
    primary_key = schema.get("primary_key")

    if primary_key:
        if df.duplicated(subset=primary_key).any():
            raise ValueError(
                f"Duplicate primary key: {primary_key}"
            )

    print("✅ Schema validation passed")
    print(f"✅ Rows: {len(df)}")
    print(f"✅ Columns: {len(df.columns)}")


if __name__ == "__main__":
    validate_schema()