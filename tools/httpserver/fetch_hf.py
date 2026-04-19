
import json
import subprocess
import sys

def install_and_import():
    try:
        import datasets
        import pandas
    except ImportError:
        print("Installing required packages...")
        subprocess.check_call([sys.executable, "-m", "pip", "install", "datasets", "pandas"])

install_and_import()

from datasets import load_dataset
import pandas as pd

items = []
count = 0

print("Attempting to download duxprajapati/symptom-disease-dataset...")
try:
    ds = load_dataset("duxprajapati/symptom-disease-dataset", split="train")
    df = ds.to_pandas()
    print(f"Loaded {len(df)} rows from duxprajapati/symptom-disease-dataset")
    for idx, row in df.iterrows():
        disease = str(row.get("disease", row.get("label", row.get("output", ""))))
        symptoms = str(row.get("symptoms", row.get("text", row.get("input", ""))))
        if disease == "nan" or symptoms == "nan": continue
        if not disease or not symptoms: continue
        i        i   {
        i        i  hf        i cou        i          i te        i       ex        i
                          e,
            "description": f"Sym            "descrip              "descripti+=            "description": f"Sym            "uxprajapati:", e)

    print("Attempting fallback to QuyenAnh/symptom2disease...")
    try:
        ds = load_dataset("Qu        ds = load_dataset("Qu ="tr        ds = load= ds.to_pandas()
        p        p        p        p        p        p        p        p       or idx,        p        p        p        p        p        p        p        p   ("        p        p    t"        p        p        p        p        p  mptoms", row.get("text", row.ge        p        p            if disease == "nan" or symptoms == "nan": continue
            if not disease or not sym            if not disease or not sym            if not disease or not ssease_{count}",
                "category": "medical_expert_hf",
                                                    sc                  ms: {                                                    sc                  ms: {        p                           )
                                                    sc                  ms: {                                                    sc     s, f,                                                     sries into {       e}")
