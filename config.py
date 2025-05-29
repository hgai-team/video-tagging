PROMPT_TEXT = '''
Please analyze the video I’ve provided and extract all tags organized into the following JSON structure. Return ONLY valid JSON (no extra fields, comments or explanations). If a field is optional (“if any”) and you do not detect it in the video, include it with an empty string ("") for string fields or an empty array ([]) for list fields:

{
  "Overview": [ /* up to 10 noun keyword about overview of its content, specifying the main objects shown, the key subjects or characters featured, and the central theme or topic it conveys */ ]
  "Entities": {                    
    "Person":    [ /* up to 4 terms about gender, age, if it is a group, type of human being e.g. "female adult", "group of teenagers", "officers", etc */ /* if any */  ],
    "Animal":    [ /* up to 2 animals */ /* if any */ ],
    "Object":    [  /* up to 5 main objects */]
  },
  "ActionsEventsFestivals": [             /
    /* up to 5 keywords about the happening activities, events, festivals, accident, etc*/
  ],
  "SceneContext": {                 
    "Type":          "",            // "indoor" or "outdoor"
    "HumanMadeArea": [ /* up to 3 terms e.g. "city street", "modern office", "historic temple", etc */ /* if any */],
    "NaturalEnvironment":    [ /* up to 3 terms e.g. "urban", "forest", "beach", etc */ /* if any */]
  },
  "TemporalContext": {              
    "PartOfDay":    " 1 keyword about what part of the day",             
    "Season":       " 1 keyword about the season",             
    "Weather":      [ 2 terms ],             
    "Lighting":    [ 2 terms about lighting e.g "backlight", "silhouette", etc]
  },
  "AffectiveMoodVibe": [ /* 3 adjectives e.g. "serene", "nostalgic", "energetic", etc */ ],
  "Color": [/* 3 terms about color palettes],
  "VisualAttributes": [ /* 3 terms about textures, patterns e.g. "grainy texture", "striped pattern", etc */ ],
  "CameraTechniques": [ /* 3 terms combining angle, movement, focus e.g. "wide shot", "pan", "rack focus", "eye-level", etc */ ]
}

'''

import os

# API Configuration
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "AIzaSyCTlwN2reyJQ6edvKSkjf31hD1n2DB7eVw")

# Processing Configuration
MAX_CONCURRENT_TASKS = 100
MAX_RETRIES = 3
RETRY_DELAY = 10
PROCESSING_TIMEOUT = 120

# Video Processing
DEFAULT_WIDTH = 800
DEFAULT_BITRATE = "1000k"
TARGET_FRAMES = 59

# Database Configuration
RESULTS_DB_PATH = "results.db"
STATUS_DB_PATH = "status.db"