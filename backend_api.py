from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
from typing import List, Dict
import os

app = FastAPI()

# Allow CORS for local frontend dev
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

CLEAN_CSV = 'clean_data_table.csv'

@app.get("/api/clean_data")
def get_clean_data() -> List[Dict]:
    if not os.path.exists(CLEAN_CSV):
        return []
    df = pd.read_csv(CLEAN_CSV)
    return df.to_dict(orient="records")
