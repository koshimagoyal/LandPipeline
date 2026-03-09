import streamlit as st
import os
import snowflake.connector
from llama_index.llms.ollama import Ollama
from dotenv import load_dotenv

load_dotenv()

# --- 1. LIGHTWEIGHT LLM SETUP ---
OLLAMA_HOST = os.getenv("OLLAMA_BASE_URL", "http://host.docker.internal:11434")
llm = Ollama(model="llama3.2:1b", base_url=OLLAMA_HOST, request_timeout=300.0)

# --- 2. SIMPLE TOOL (MCP Logic) ---
def get_snowflake_data(parcel_id: str):
    """Direct fetch from Snowflake (The 'Tool')"""
    try:
        conn = snowflake.connector.connect(
            user=os.getenv("SNOWFLAKE_USER"),
            password=os.getenv("SNOWFLAKE_PASSWORD"),
            account=os.getenv("SNOWFLAKE_ACCOUNT"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
            database=os.getenv("SNOWFLAKE_DATABASE"),
            schema=os.getenv("SNOWFLAKE_SCHEMA")
        )
        cur = conn.cursor()
        cur.execute(f"SELECT VALUATION, AI_ZONING_SUMMARY, MARKET_SENTIMENT FROM LAND_INTELLIGENCE_INSIGHTS WHERE ID = '{parcel_id}'")
        row = cur.fetchone()
        conn.close()
        return row if row else None
    except Exception as e:
        return f"Error: {str(e)}"

# --- 3. STREAMLIT UI ---
st.title("🌍 Terra-Agent: Light Edition")
parcel_id = st.text_input("Enter Parcel ID", "P101")

if st.button("Run Intelligence Report"):
    with st.spinner("Fetching data and summarizing..."):
        
        # STEP 1: Manual Tool execution (reliable for 1B models)
        data = get_snowflake_data(parcel_id)
        
        if data and not isinstance(data, str):
            val, zoning, sentiment = data
            
            # STEP 2: Minimalist Prompt (Easy for 1B models)
            prompt = f"""
            Data for Parcel {parcel_id}:
            - Valuation: ${val}
            - Zoning Info: {zoning}
            - Sentiment Score: {sentiment}

            As an AI Real Estate Agent, provide a 2-sentence risk assessment.
            """
            
            # STEP 3: Simple Completion (No ReAct loop)
            response = llm.complete(prompt)
            
            st.subheader("Agent Assessment")
            st.write(response.text)
            
            with st.expander("View Source Data"):
                st.json({"valuation": val, "zoning": zoning, "sentiment": sentiment})
        else:
            st.error("No data found or connection error.")