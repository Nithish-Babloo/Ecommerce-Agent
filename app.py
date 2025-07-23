import os
import pandas as pd
import matplotlib
matplotlib.use('Agg') # Use a non-interactive backend for Matplotlib
import matplotlib.pyplot as plt
import base64
from io import BytesIO
# Import render_template to serve the HTML page
from flask import Flask, request, jsonify, Response, render_template
from sqlalchemy import create_engine
from langchain_community.utilities import SQLDatabase
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain.chains import create_sql_query_chain
from langchain_community.tools import QuerySQLDatabaseTool
import json
import sys
from dotenv import load_dotenv

# Load the environment variables from the .env file
load_dotenv() 

# Basic Flask app setup
app = Flask(__name__)

# Database connection
db_path = 'sqlite:///ecommerce.db'
db = SQLDatabase.from_uri(db_path)
engine = create_engine(db_path)

# LLM and LangChain configuration
try:
    api_key = os.environ.get("GOOGLE_API_KEY")
    if not api_key:
        print("ERROR: GOOGLE_API_KEY not found in .env file.", file=sys.stderr)
        sys.exit(1)
        
    llm = ChatGoogleGenerativeAI(model="gemini-1.5-flash", google_api_key=api_key, temperature=0)
    
    generate_query_chain = create_sql_query_chain(llm, db)
    execute_query_tool = QuerySQLDatabaseTool(db=db)

except Exception as e:
    print(f"Error initializing LLM: {e}. Ensure GOOGLE_API_KEY is set correctly in your .env file.", file=sys.stderr)
    sys.exit(1)

# New route to serve the frontend web page
@app.route('/')
def index():
    """Serves the frontend web page."""
    return render_template('index.html')

@app.route('/query', methods=['POST'])
def handle_query():
    """Handles natural language queries and streams back the SQL, answer, and chart."""
    data = request.get_json()
    if not data or 'question' not in data:
        return jsonify({"error": "Question not provided"}), 400
    question = data['question']

    def event_stream():
        """Generator function to stream the response."""
        try:
            # Step 1: Generate SQL query from the LLM
            generated_query = generate_query_chain.invoke({"question": question})

            # Clean the generated query to remove any unwanted prefixes
            sql_query = generated_query.strip()
            if sql_query.upper().startswith("SQLQUERY:"):
                sql_query = sql_query[len("SQLQUERY:"):].strip()

            # Stream the cleaned SQL query
            yield f"data: {json.dumps({'type': 'sql', 'content': sql_query})}\n\n"

            # Step 2: Execute the CLEANED SQL query to get the answer
            answer = execute_query_tool.invoke({"query": sql_query})
            yield f"data: {json.dumps({'type': 'answer', 'content': answer.strip()})}\n\n"

            # Step 3: (Bonus) Generate chart if applicable
            chart_keywords = ['per product', 'by category', 'for each', 'list the sales', 'show me the']
            if any(keyword in question.lower() for keyword in chart_keywords):
                try:
                    df = pd.read_sql_query(sql_query, engine)
                    if df.shape[1] >= 2 and pd.api.types.is_numeric_dtype(df.iloc[:, 1]):
                        plt.figure(figsize=(10, 6))
                        plt.bar(df.iloc[:, 0].astype(str), df.iloc[:, 1])
                        plt.title(f'Chart for: "{question}"')
                        plt.xlabel(df.columns[0])
                        plt.ylabel(df.columns[1])
                        plt.xticks(rotation=45, ha='right')
                        plt.tight_layout()

                        buf = BytesIO()
                        plt.savefig(buf, format="png")
                        image_base64 = base64.b64encode(buf.getvalue()).decode('utf-8')
                        plt.close()
                        yield f"data: {json.dumps({'type': 'chart', 'content': image_base64})}\n\n"
                except Exception as chart_error:
                    print(f"Could not generate chart: {chart_error}", file=sys.stderr)

        except Exception as e:
            yield f"data: {json.dumps({'type': 'error', 'content': str(e)})}\n\n"
        
        # Signal the end of the stream
        yield f"data: {json.dumps({'type': 'end'})}\n\n"

    return Response(event_stream(), mimetype='text/event-stream')

if __name__ == '__main__':
    app.run(debug=True, port=5000)
