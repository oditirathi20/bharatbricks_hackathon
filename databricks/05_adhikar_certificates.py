"""
Adhikar-Aina | Adhikar Certificates

Purpose:
- Generate citizen-facing entitlement certificates from eligibility results.

Creates:
- workspace.default.aa_adhikar_certificates

Runtime configuration:
- Requires ANTHROPIC_API_KEY from Databricks environment or secret scope.
"""


import pyspark.sql.functions as F
from pyspark.sql.types import *
import requests
import json
import os

spark.sql("USE CATALOG workspace")
spark.sql("USE default")

ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")

if not ANTHROPIC_API_KEY:
    raise ValueError("ANTHROPIC_API_KEY is not set. Configure it in Databricks environment or secret scope.")

test = requests.post(
    "https://api.anthropic.com/v1/messages",
    headers={
        "Content-Type": "application/json",
        "x-api-key": ANTHROPIC_API_KEY,
        "anthropic-version": "2023-06-01"
    },
    json={
        "model": "claude-sonnet-4-20250514",
        "max_tokens": 50,
        "messages": [{"role": "user", "content": "Reply with just: OK"}]
    },
    timeout=30
)
print("API test status :", test.status_code)
print("API test response:", test.json()["content"][0]["text"])

df_results  = spark.table("workspace.default.aa_eligibility_results")
df_citizens = spark.table("workspace.default.aa_citizens_for_llm")

df_sample = (
    df_results
    .groupBy("citizen_id", "district", "village", "taluka", "income_bracket", "caste_category", "is_tribal")
    .agg(
        F.count("scheme_id").alias("total_schemes"),
        F.collect_list("scheme_name").alias("scheme_names"),
        F.collect_list("benefit").alias("benefits"),
        F.collect_list("short_code").alias("short_codes"),
    )
    .orderBy(F.desc("total_schemes"))
    .limit(10)
)

rows = df_sample.collect()
certificates = []

for row in rows:
    citizen_id     = row["citizen_id"]
    district       = row["district"]
    village        = row["village"]
    taluka         = row["taluka"]
    income_bracket = row["income_bracket"]
    caste_category = row["caste_category"]
    is_tribal      = row["is_tribal"]
    scheme_names   = row["scheme_names"]
    benefits       = row["benefits"]
    short_codes    = row["short_codes"]
    total_schemes  = row["total_schemes"]

    top_schemes = list(zip(short_codes[:5], scheme_names[:5], benefits[:5]))
    scheme_text = "\n".join([
        f"  - {sc}: {name[:60]} | Benefit: {str(ben)[:80]}"
        for sc, name, ben in top_schemes
    ])

    prompt = f"""You are a legal AI for India's Adhikar-Aina welfare system.
Generate a formal Adhikar Certificate proving this citizen's right to government schemes.

CITIZEN PROFILE:
- Citizen ID : {citizen_id}
- District   : {district}, Taluka: {taluka}, Village: {village}
- Income     : {income_bracket}
- Caste      : {caste_category}
- Tribal     : {"Yes" if is_tribal else "No"}
- Total Eligible Schemes: {total_schemes}

TOP 5 MATCHED SCHEMES:
{scheme_text}

Return ONLY this JSON, no markdown, no explanation:
{{
  "certificate_id": "AC-{district[:3].upper()}-{citizen_id[:4].upper()}",
  "citizen_id": "{citizen_id}",
  "issued_by": "Adhikar-Aina Sovereign AI System",
  "issued_date": "2026-03-28",
  "legal_validity": "Generated under Right to Information Act 2005 and relevant scheme guidelines",
  "citizen_summary": "2 sentence summary of citizen profile",
  "top_schemes": [
    {{
      "scheme_code": "code",
      "scheme_name": "name",
      "benefit": "benefit",
      "legal_basis": "relevant act",
      "action_required": "what citizen must do"
    }}
  ],
  "marathi_message": "1 sentence in Marathi about their rights",
  "hindi_message": "1 sentence in Hindi about their rights",
  "total_schemes_eligible": {total_schemes},
  "disclaimer": "AI-generated based on declared data. Subject to verification."
}}"""

    try:
        response = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "Content-Type": "application/json",
                "x-api-key": ANTHROPIC_API_KEY,
                "anthropic-version": "2023-06-01"
            },
            json={
                "model": "claude-sonnet-4-20250514",
                "max_tokens": 1000,
                "messages": [{"role": "user", "content": prompt}]
            },
            timeout=60
        )

        raw_text = response.json()["content"][0]["text"].strip()
        raw_text = raw_text.replace("```json", "").replace("```", "").strip()
        cert     = json.loads(raw_text)
        cert_str = json.dumps(cert)
        print(f"✅ {cert.get('certificate_id')} — {district}/{village}")

    except Exception as e:
        print(f"❌ Failed for {citizen_id}: {e}")
        print(f"   Raw: {response.text[:300] if 'response' in locals() else 'no response'}")
        cert_str = json.dumps({"error": str(e), "citizen_id": citizen_id})

    certificates.append((
        citizen_id,
        district,
        village,
        int(total_schemes),
        cert_str,
    ))

cert_schema = StructType([
    StructField("citizen_id",          StringType(),  False),
    StructField("district",            StringType(),  True),
    StructField("village",             StringType(),  True),
    StructField("total_schemes",       IntegerType(), True),
    StructField("adhikar_certificate", StringType(),  True),
])

df_certs = (
    spark.createDataFrame(certificates, schema=cert_schema)
    .withColumn("generated_at", F.current_timestamp())
)

(df_certs.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("workspace.default.aa_adhikar_certificates"))

print("✅ aa_adhikar_certificates written")

sample = spark.table("workspace.default.aa_adhikar_certificates").limit(1).collect()[0]
cert   = json.loads(sample["adhikar_certificate"])

print("\n" + "="*60)
print("         ADHIKAR CERTIFICATE — SAMPLE OUTPUT")
print("="*60)
print(f"Certificate ID  : {cert.get('certificate_id')}")
print(f"Citizen ID      : {cert.get('citizen_id')}")
print(f"Issued By       : {cert.get('issued_by')}")
print(f"Issued Date     : {cert.get('issued_date')}")
print(f"Total Eligible  : {cert.get('total_schemes_eligible')} schemes")
print(f"\nCitizen Summary : {cert.get('citizen_summary')}")
print(f"\nMarathi Message : {cert.get('marathi_message')}")
print(f"Hindi Message   : {cert.get('hindi_message')}")
print(f"\nLegal Validity  : {cert.get('legal_validity')}")
print("\nTop Schemes:")
for s in cert.get("top_schemes", []):
    print(f"  [{s.get('scheme_code')}] {s.get('scheme_name')[:50]}")
    print(f"    Benefit : {s.get('benefit')}")
    print(f"    Action  : {s.get('action_required')}")
print("="*60)
print(f"\nDisclaimer: {cert.get('disclaimer')}")
print("\n✅ Notebook 05 done — Adhikar Certificates live")
print("▶ Ready for NB06 — Databricks Workflow automation")