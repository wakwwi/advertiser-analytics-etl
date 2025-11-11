from pathlib import Path
from dotenv import load_dotenv
import os

load_dotenv()

PROJ = Path(__file__).resolve().parents[1]
DATA = PROJ / os.getenv("DATA_DIR", "data")
OUT  = PROJ / os.getenv("OUTPUT_DIR", "output")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
