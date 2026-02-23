"""Blockerar frågor inom exkluderade rättsområden."""
import yaml

def load_excluded_areas(config_path: str = "config/excluded_areas.yaml") -> list[dict]:
    with open(config_path) as f:
        return yaml.safe_load(f)["excluded_areas"]

def check_blocked(question: str, sfs_nr: str | None = None) -> dict | None:
    """Returnerar dict med 'blocked':True och 'message' om blockerad, annars None."""
    areas = load_excluded_areas()
    q_lower = question.lower()
    for area in areas:
        if sfs_nr and any(sfs_nr.startswith(p) for p in area.get("sfs_patterns", [])):
            return {"blocked": True, "area": area["label"], "message": area["message"]}
        keywords = area.get("keywords", [])
        if any(kw in q_lower for kw in keywords):
            return {"blocked": True, "area": area["label"], "message": area["message"]}
    return None
