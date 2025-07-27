#!/usr/bin/env python3
import os
import psycopg2
import evaluate
from summarize import get_structured_data, get_unstructured_data, format_prompt, generate
from qdrant_client import QdrantClient
from sentence_transformers import SentenceTransformer

# --- CONFIG ---
PG_DSN = os.environ.get("PG_DSN", "host=localhost dbname=synthea user=mimic password=strong_password")

# --- GOLD STANDARD SUMMARIES ---
# In a real scenario, these would be loaded from a file.
GOLD_SUMMARIES = {
    "<episode_id_1>": "This is a gold standard summary for episode 1.",
    "<episode_id_2>": "This is a gold standard summary for episode 2.",
}

def main():
    # --- Initialize clients ---
    model = SentenceTransformer('intfloat/e5-base-v2')
    qdrant_client = QdrantClient(os.environ.get("QDRANT_URL", "http://localhost:6333"))

    # --- Get episode IDs from the gold standard set ---
    episode_ids = list(GOLD_SUMMARIES.keys())

    # --- Generate summaries ---
    generated_summaries = []
    for ep_id in episode_ids:
        structured_data = get_structured_data(ep_id)
        unstructured_data = get_unstructured_data(ep_id, model, qdrant_client)
        prompt = format_prompt(ep_id, structured_data, unstructured_data)
        summary = generate(prompt)
        generated_summaries.append(summary)

    # --- Evaluate ---
    rouge = evaluate.load('rouge')
    results = rouge.compute(predictions=generated_summaries, references=list(GOLD_SUMMARIES.values()))

    print("--- Evaluation Results ---")
    print(results)

if __name__ == "__main__":
    main()
