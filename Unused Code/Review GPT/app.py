from fastapi import FastAPI, Form
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from openai import OpenAI
from config import OPENAI_API_KEY
import asyncio

client = OpenAI(api_key=OPENAI_API_KEY)

app = FastAPI()

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/", response_class=HTMLResponse)
async def root():
    with open("index.html") as f:
        return f.read()

@app.post("/api/chat")
async def chat(message: str = Form(...)):
    try:
        response = client.chat.completions.create(
            model="gpt-4",
            messages=[
                {"role": "system", "content": "You are a helpful assistant."},
                {"role": "user", "content": message}
            ]
        ),
        timeout=30.0
        return {"response": response.choices[0].message.content}
    except asyncio.TimeoutError:
        return {"error": "Request timed out"}
    except Exception as e:
        return {"error": str(e)}