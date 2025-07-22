VIDEO_TAGGING_PROMPT = '''
Please analyze the video I’ve provided and extract all tags organized into the following JSON structure. Return ONLY valid JSON (no extra fields, comments or explanations). If a field is unclear or not detected, provide an empty list ([]):

{
  "overview": [ /* up to 10 noun keywords specifying the main objects shown, key subjects or characters featured, and central themes or topics */ ],

  "entities": {
    "person": [ /* up to 4 terms about gender, age, group type (e.g., "female adult", "group of teenagers", "officers") */ ],
    "animal": [ /* up to 2 animals */ ],
    "object": [ /* up to 5 main objects */ ]
  },

  "actions_events_festivals": [ /* up to 5 keywords about occurring activities, events, festivals, accidents */ ],

  "scene_context": {
    "type": [ /* 1 term: "indoor" or "outdoor" */ ],
    "human_made_area": [ /* up to 3 terms (e.g., "city street", "modern office", "historic temple") */ ],
    "natural_environment": [ /* up to 3 terms (e.g., "urban", "forest", "beach") */ ]
  },

  "temporal_context": {
    "part_of_day": [ /* 1 keyword about the part of the day */ ],
    "season": [ /* 1 keyword about the season */ ],
    "weather": [ /* up to 2 terms */ ],
    "lighting": [ /* up to 2 terms about lighting (e.g., "backlight", "silhouette") */ ]
  },

  "affective_mood_vibe": [ /* 3 adjectives (e.g., "serene", "nostalgic", "energetic") */ ],
  "color": [ /* 3 terms about color palettes */ ],
  "visual_attributes": [ /* 3 terms about textures, patterns (e.g., "grainy texture", "striped pattern") */ ],
  "camera_techniques": [ /* 3 terms combining angle, movement, focus (e.g., "wide shot", "pan", "rack focus", "eye-level") */ ],

  "video_description": /* 1 paragraph comprehensively describing the entire video */
}
'''

VIDEO_DETECTION_PROMPT = '''
You are a Video Reality & AI Integration Detector.
Your mission is to analyze this video and determine:

STEP 1: First, determine if this is a real-life scene or live footage in reality that filmed by human (answer "true" or "false")
STEP 2: Second, determine if the video has AI-generated imagery integrated into it (hybrid layers between real footage and AI-generated content) or 100% AI-generated (answer "true" or "false")

Return your response in JSON format with:
- "is_real_life": true/false (based on step 1). If you are not uncertained and sured, return false.
- "has_ai_elements": true/false (based on step 2)

Example output: {"is_real_life": true/false, "has_ai_elements": false/true}
'''

AUDIO_TAGGING_PROMPT = '''
You are a music analysis expert at a company specializing in long-form YouTube music. Your tasks are:

1. Analyze the provided audio file and extract descriptive keywords of the music.  
2. Determine which project categories this music best fits within our lineup (e.g. Acoustic, Ambience, Classical, EDM, Jazz, Kids Music, Lofi, Phonk, Gospel/Worship, Pop, Relax, Binaural Beats/Alpha Waves, Cinematic, Indie, Whitenoise, R&B etc.; you may suggest additional types but prioritize these).  
3. Provide a detailed description of the audio’s characteristics—melodic motifs, instrumentation, mood shifts, textures, etc.—and store it in the `"video_description"` field.

Return the result in exactly this JSON format:
```
{
  "genre": ["…"],
  "emotion": ["…"],
  "instruments": ["…"],
  "tempo": ["…"],
  "sfx": ["…"],
  "project": ["…"]
  "audio_description": "…",
}

NOTE: Keywords identified must be higly confident. Quality is important than Quantity.
'''