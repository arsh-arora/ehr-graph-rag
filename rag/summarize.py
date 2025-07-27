#!/usr/bin/env python3
import os
import psycopg2
from neo4j import GraphDatabase, basic_auth
from qdrant_client import QdrantClient
from sentence_transformers import SentenceTransformer
import openai

# --- CONFIG ---
PG_DSN = os.environ.get("PG_DSN", "host=localhost dbname=synthea user=mimic password=strong_password")
NEO_URI = os.environ.get("NEO_URI", "bolt://localhost:7687")
NEO_AUTH = basic_auth(os.environ.get("NEO4J_USER", "neo4j"), os.environ.get("NEO4J_PASSWORD", "neo4j_password"))
QDRANT_URL = os.environ.get("QDRANT_URL", "http://localhost:6333")
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")

# --- LLM ---
def generate(prompt, max_tokens=700, temperature=0.2):
    if not OPENAI_API_KEY:
        raise ValueError("OPENAI_API_KEY environment variable not set")
    openai.api_key = OPENAI_API_KEY
    response = openai.ChatCompletion.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": "You are a careful clinical summarizer."},
            {"role": "user", "content": prompt}
        ],
        temperature=temperature,
        max_tokens=max_tokens
    )
    return response.choices[0].message['content']

# --- DATA RETRIEVAL ---
def get_structured_data(ep_id):
    with GraphDatabase.driver(NEO_URI, auth=NEO_AUTH) as driver:
        with driver.session() as session:
            result = session.run("""
                MATCH (e:Episode {ep_id: $ep_id})-[r]->(x)
                RETURN e, r, x
            """, ep_id=ep_id)
            return result.data()

def get_unstructured_data(ep_id, model, client, collection_name="notes_chunks_dev"):
    query_vector = model.encode([f"[query] Clinical notes for episode {ep_id}"], normalize_embeddings=True)[0]
    hits = client.search(
        collection_name=collection_name,
        query_vector=query_vector,
        limit=10,
        query_filter={"must": [{"key": "ep_id", "match": {"value": ep_id}}]}
    )
    return hits

# --- PROMPT ENGINEERING ---
def format_prompt(ep_id, structured_data, unstructured_data):
    # This is a simplified example. You would format the data more nicely.
    prompt = f"""
    Generate a clinical summary for the patient episode: {ep_id}

    **Structured Episode Data:**
    {structured_data}

    **Unstructured Clinical Notes (ranked by relevance):**
    """
    for hit in unstructured_data:
        prompt += f"--- Note (Timestamp: {hit.payload['ts']}, Section: {hit.payload['section']}) ---\n"
        prompt += f"{hit.payload['text']}\n"

    prompt += """
    **Task:**
    Based ONLY on the information provided above, generate a summary covering the following sections:
    1. Presenting Problem
    2. Hospital Course
    3. Key Medical History
    4. Discharge Summary

    **Constraints:**
    - Do not infer or add any information not present in the provided context.
    - Be concise and use clear medical terminology.
    - If a section cannot be filled due to lack of information, state 'Information not available.'
    """
    return prompt

def main():
    # --- Get a sample episode ID ---
    with psycopg2.connect(PG_DSN) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT ep_id FROM coh.episodes LIMIT 1")
            ep_id = cur.fetchone()[0]

    # --- Initialize clients ---
    model = SentenceTransformer('intfloat/e5-base-v2')
    qdrant_client = QdrantClient(QDRANT_URL)

    # --- Retrieve data ---
    structured_data = get_structured_data(ep_id)
    unstructured_data = get_unstructured_data(ep_id, model, qdrant_client)

    # --- Generate prompt and summary ---
    prompt = format_prompt(ep_id, structured_data, unstructured_data)
    summary = generate(prompt)

    print(f"--- Summary for Episode: {ep_id} ---")
    print(summary)

if __name__ == "__main__":
    main()
