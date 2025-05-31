import re
import json

def json_parser(
    text: str
):
    try:
        json_match = re.search(r'```(?:json)?\s*([\s\S]*?)\s*```', text, re.DOTALL)
        if json_match:
            json_str = json_match.group(1).strip()
        else:
            json_str = text.strip()
        json_str = re.sub(r',\s*([}\]])', r'\1', json_str)
        result = json.loads(json_str)
        return result
    except Exception:
        return {
            "status": "error",
            "response": "Failed to parse analysis.",
            "raw_analysis": text
        }
