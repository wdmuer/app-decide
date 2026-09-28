#!/usr/bin/env python3
"""Write the e2e NER config: the committed config with translation on the Mistral API."""
import json
from pathlib import Path

SOURCE = Path("config/ner/config.json")
TARGET = Path("data/e2e/ner-config.json")

config = json.loads(SOURCE.read_text(encoding="utf-8"))
config["translation"]["langchain"].update(
    provider="mistralai",
    model_name="mistral-medium-3.5",
    base_url="https://api.mistral.ai/v1",
)
TARGET.parent.mkdir(parents=True, exist_ok=True)
TARGET.write_text(json.dumps(config, indent=2), encoding="utf-8")
print(f"Wrote {TARGET}")
