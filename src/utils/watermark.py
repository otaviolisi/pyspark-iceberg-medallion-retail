import json
from pathlib import Path


def read_watermark(file_path: str, default_value: str) -> str:
    path = Path(file_path)

    if not path.exists():
        path.parent.mkdir(parents=True, exist_ok=True)
        return default_value

    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    return data.get("watermark", default_value)


def write_watermark(file_path: str, watermark_value: str) -> None:
    path = Path(file_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    with open(path, "w", encoding="utf-8") as f:
        json.dump({"watermark": watermark_value}, f, ensure_ascii=False, indent=2)