# insight_config.py

# how many candidates we WANT to keep per polarity before AI ranking
TARGET_STRENGTH_CANDIDATES = 30
TARGET_WEAKNESS_CANDIDATES = 30

# how many final insights show on dashboard
FINAL_STRENGTHS_TO_SHOW = 3
FINAL_WEAKNESSES_TO_SHOW = 3

# if a polarity doesn't have enough, allow fewer rather than making junk
ALLOW_FEWER_IF_NOT_ENOUGH = True

# max candidates we ever send to Mistral (cost control)
MAX_CANDIDATES_PER_MANAGER = 200

# --- delta guardrails ---
MIN_PREV_FOR_DELTA = 5          # if prev window total < this, % delta is noisy
MAX_ABS_DP_FOR_SEVERITY = 5.0   # cap at 500% for severity scoring only
