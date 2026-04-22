import os
import re
import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
from dotenv import load_dotenv
from langchain_openai import ChatOpenAI
import matplotlib.pyplot as plt

# -----------------------------
# Load ENV
# -----------------------------
load_dotenv()

username = quote_plus(os.getenv("DB_USER"))
password = quote_plus(os.getenv("DB_PASS"))
server = os.getenv("DB_SERVER")
database = os.getenv("DB_NAME")

connection_string = (
    f"mssql+pyodbc://{username}:{password}@{server}/{database}"
    "?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes"
)

engine = create_engine(connection_string)

# -----------------------------
# LLM
# -----------------------------
llm = ChatOpenAI(
    model="gpt-4o-mini",
    temperature=0,
    api_key=os.getenv("OPENAI_API_KEY")
)

# -----------------------------
# Security Layer
# -----------------------------
ALLOWED_TABLES = ["DEALS_BREAKUP_TBL"]
FORBIDDEN = ["DROP", "DELETE", "UPDATE", "INSERT", "ALTER"]

def safe_query(query):
    q = query.upper()

    if any(word in q for word in FORBIDDEN):
        return False, "❌ Unsafe query blocked"

    if not q.startswith("SELECT"):
        return False, "❌ Only SELECT allowed"

    tables = re.findall(r'FROM\s+(\w+)', q)
    for t in tables:
        if t not in ALLOWED_TABLES:
            return False, f"❌ Table {t} not allowed"

    return True, "OK"

def run_query(query):
    ok, msg = safe_query(query)
    if not ok:
        return msg

        # If TOP already exists, do nothing
    

    # Handle DISTINCT case
    #if "DISTINCT" in query:
        #query=query.replace("SELECT", f"SELECT DISTINCT TOP 100")

    # Normal SELECT
    #if "DISTINCT" not in query:
        #query=query.replace("SELECT", f"SELECT TOP 100")

    #if "TOP" not in query.upper():
        #query = query.replace("SELECT", "SELECT TOP 100")

    try:
        with engine.connect() as conn:
            result = conn.execute(text(query))
            df = pd.DataFrame(result.fetchall(), columns=result.keys())
            return df
    except Exception as e:
        return f"Error: {e}"

# -----------------------------
# MCP Tool
# -----------------------------
tools = [{
    "type": "function",
    "function": {
        "name": "query_database",
        "description": "Query DEALS_BREAKUP_TBL",
        "parameters": {
            "type": "object",
            "properties": {
                "query": {"type": "string"}
            },
            "required": ["query"]
        }
    }
}]

SYSTEM_PROMPT = """
You are a SQL assistant.

Use table DEALS_BREAKUP_TBL.
Columns: ACID,LIMIT_B2KID,BRCA,CUST_ID,CUST_NAME,ACOD,ACOD_DESC,DTYPE,REFERENCE,gldesc,ValDate,Matdate,RollDate,CCY,AMOUNT,
            amountsgd,INT_RATE,TOTAL_INT,TOTAL_INT_SGD,ACCRUED_INT,ACCRUED_INT_SGD,BaseIntRateType,InterestMargin,AmortizedInterest,LinkReference,
            BANK_NONBANK,CUST_TYPE,CUST_GRP,RES_NR,Risk_Country,RESIDENCE,RESIDENCE_NAME,NATIONALITY,NATIONALITY_NAME,SECURED,LoanPurpose,
            INDUSTRY_CODE,INDUSTRY_NAME,INST_CODE,INSTITUION_NAME,RunDate,AddonDetail

Always generate SQL Server queries.
"""

# -----------------------------
# Streamlit UI
# -----------------------------
st.title("📊 AI SQL Dashboard (MCP Style)")

question = st.text_input("Ask a question")

if st.button("Run Query"):

    response = llm.invoke(
        [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": question}
        ],
        tools=tools
    )

    if response.tool_calls:
        sql_query = response.tool_calls[0]["args"]["query"]

        st.subheader("🧠 Generated SQL")
        st.code(sql_query)

        result = run_query(sql_query)

        if isinstance(result, str):
            st.error(result)
        else:
            st.subheader("📋 Data")
            st.dataframe(result)

            # -----------------------------
            # Auto Chart Detection
            # -----------------------------
            if len(result.columns) >= 2:
                x = result.columns[0]
                y = result.columns[1]

                st.subheader("📊 Chart")

                fig, ax = plt.subplots()
                ax.bar(result[x], result[y])
                plt.xticks(rotation=45)

                st.pyplot(fig)

    else:
        st.write(response.content)