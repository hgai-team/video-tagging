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

