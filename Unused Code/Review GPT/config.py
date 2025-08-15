from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# Retrieve API Key
OPENAI_API_KEY = os.getenv('OPENAI_API_KEY')

# Improved error handling
if not OPENAI_API_KEY:
    raise ValueError("OpenAI API key not found. Ensure it's set in the .env file.")