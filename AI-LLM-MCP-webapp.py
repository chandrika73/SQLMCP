import re
import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text
from langchain_openai import ChatOpenAI
import matplotlib.pyplot as plt

# -----------------------------
# DB Connection (Neon) Postgres
# -----------------------------
engine = create_engine(st.secrets["DATABASE_URL"])

# -----------------------------
# LLM
# -----------------------------
llm = ChatOpenAI(
    model="gpt-4o-mini",
    temperature=0,
    api_key=st.secrets["OPENAI_API_KEY"]
)

# -----------------------------
# Security Layer
# -----------------------------
# PostgreSQL → table name is case-sensitive
ALLOWED_TABLES = ['"Deals_Breakup_tbl"']

FORBIDDEN = ["DROP", "DELETE", "UPDATE", "INSERT", "ALTER"]

def safe_query(query):
    q = query.upper()

    if any(word in q for word in FORBIDDEN):
        return False, "❌ Unsafe query blocked"

    if not q.startswith("SELECT"):
        return False, "❌ Only SELECT allowed"

    # allow quoted table names
    tables = re.findall(r'FROM\s+["]?(\w+)["]?', q)

    for t in tables:
        if t.upper() != "DEALS_BREAKUP_TBL":
            return False, f"❌ Table {t} not allowed"

    return True, "OK"


def run_query(query):
    ok, msg = safe_query(query)
    if not ok:
        return msg

    try:
        with engine.connect() as conn:
            result = conn.execute(text(query))
            df = pd.DataFrame(result.fetchall(), columns=result.keys())
            return df
    except Exception as e:
        return f"Error: {e}"

def extract_sql(query):
# Remove markdown
    raw_text = re.sub(r"```sql|```", "", raw_text, flags=re.IGNORECASE)

    # Extract SELECT query
    match = re.search(r"(SELECT[\s\S]+?;)", raw_text, re.IGNORECASE)

    if match:
        return match.group(1).strip()

    return raw_text.strip()

# -----------------------------
# LLM Prompt (UPDATED)
# -----------------------------
SYSTEM_PROMPT = """
You are a SQL assistant.

Use PostgreSQL syntax.

Table name: "Deals_Breakup_tbl"

Rules:
- Use LIMIT instead of TOP
- Use double quotes for table name
- Do NOT use SQL Server syntax

Columns:
ACID, LIMIT_B2KID, BRCA, CUST_ID, CUST_NAME, ACOD, ACOD_DESC, DTYPE, REFERENCE,
gldesc, ValDate, Matdate, RollDate, CCY, AMOUNT, amountsgd, INT_RATE,
TOTAL_INT, TOTAL_INT_SGD, ACCRUED_INT, ACCRUED_INT_SGD, BaseIntRateType,
InterestMargin, AmortizedInterest, LinkReference, BANK_NONBANK, CUST_TYPE,
CUST_GRP, RES_NR, Risk_Country, RESIDENCE, RESIDENCE_NAME, NATIONALITY,
NATIONALITY_NAME, SECURED, LoanPurpose, INDUSTRY_CODE, INDUSTRY_NAME,
INST_CODE, INSTITUION_NAME, RunDate, AddonDetail
"""

# -----------------------------
# UI
# -----------------------------
st.title("📊 AI SQL Dashboard (PostgreSQL)")

question = st.text_input("Ask a question")

if st.button("Run Query"):

    response = llm.invoke([
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": question}
    ])

    #sql_query = response.content.strip()
    sql_query = extract_sql(response.content)

    st.subheader("🧠 Generated SQL")
    st.code(sql_query)

    result = run_query(sql_query)

    if isinstance(result, str):
        st.error(result)
    else:
        st.subheader("📋 Data")
        st.dataframe(result)

        # -----------------------------
        # Chart
        # -----------------------------
        if len(result.columns) >= 2:
            x = result.columns[0]
            y = result.columns[1]

            st.subheader("📊 Chart")

            fig, ax = plt.subplots()
            ax.bar(result[x], result[y])
            plt.xticks(rotation=45)

            st.pyplot(fig)