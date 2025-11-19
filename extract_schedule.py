import os
import base64
import json
import fitz  # pymupdf
from dotenv import load_dotenv
from openai import OpenAI
from google import genai
from google.genai import types
from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime
import typer
from enum import Enum

# Load environment variables
load_dotenv()

app = typer.Typer()

class Provider(str, Enum):
    OPENAI = "openai"
    GEMINI = "gemini"

class BiddingExercise(BaseModel):
    month: str
    exercise_start_datetime: datetime
    exercise_end_datetime: datetime

class ScheduleResponse(BaseModel):
    schedule: List[BiddingExercise]

def encode_image(pix):
    """Encodes a PyMuPDF pixmap to base64 string."""
    return base64.b64encode(pix.tobytes("png")).decode("utf-8")

def convert_pdf_to_images(pdf_path: str) -> List[str]:
    """Converts PDF pages to base64 encoded images."""
    print(f"Converting {pdf_path} to images...")
    doc = fitz.open(pdf_path)
    images = []
    for page_num in range(len(doc)):
        page = doc.load_page(page_num)
        pix = page.get_pixmap(matrix=fitz.Matrix(2, 2)) # 2x zoom for better resolution
        images.append(encode_image(pix))
    doc.close()
    return images

def extract_with_openai(images: List[str], prompt_text: str) -> Optional[ScheduleResponse]:
    """Extracts schedule using OpenAI."""
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        print("Error: OPENAI_API_KEY not found.")
        return None
        
    client = OpenAI(api_key=api_key)
    
    messages = [
        {
            "role": "system",
            "content": "You are a helpful assistant that extracts data from documents."
        },
        {
            "role": "user",
            "content": [
                {"type": "text", "text": prompt_text},
            ]
        }
    ]

    for img_b64 in images:
        messages[1]["content"].append({
            "type": "image_url",
            "image_url": {
                "url": f"data:image/png;base64,{img_b64}"
            }
        })

    try:
        print("Sending request to OpenAI...")
        response = client.beta.chat.completions.parse(
            model="gpt-4o",
            messages=messages,
            response_format=ScheduleResponse,
        )
        return response.choices[0].message.parsed
    except Exception as e:
        print(f"Error extracting with OpenAI: {e}")
        return None

def extract_with_gemini(images: List[str], prompt_text: str) -> Optional[ScheduleResponse]:
    """Extracts schedule using Google Gemini."""
    api_key = os.getenv("GEMINI_API_KEY")
    if not api_key:
        print("Error: GEMINI_API_KEY not found.")
        return None

    client = genai.Client(api_key=api_key)

    parts = [types.Part.from_text(text=prompt_text)]
    
    for img_b64 in images:
        parts.append(types.Part.from_bytes(
            data=base64.b64decode(img_b64),
            mime_type="image/png"
        ))

    try:
        print("Sending request to Gemini...")
        response = client.models.generate_content(
            model="gemini-2.0-flash",
            contents=[types.Content(role="user", parts=parts)],
            config=types.GenerateContentConfig(
                response_mime_type="application/json",
                response_schema=ScheduleResponse
            )
        )
        
        # Gemini returns a JSON string that needs to be parsed into the Pydantic model
        if response.text:
            data = json.loads(response.text)
            return ScheduleResponse(**data)
        return None
        
    except Exception as e:
        print(f"Error extracting with Gemini: {e}")
        return None

def save_to_jsonl(schedule_data: ScheduleResponse, output_dir: str) -> str:
    """Saves the schedule data to a JSONL file and returns the file path."""
    if not schedule_data or not schedule_data.schedule:
        print("No data to save.")
        return None

    # Determine year from the first entry
    first_entry = schedule_data.schedule[0]
    year = first_entry.exercise_start_datetime.year
    
    output_filename = f"COE_Bidding_Schedule_{year}.jsonl"
    output_path = os.path.join(output_dir, output_filename)
    
    print(f"Saving to {output_path}...")
    
    with open(output_path, "w") as f:
        for entry in schedule_data.schedule:
            # Convert model to dict with JSON-compatible types (handles datetimes)
            entry_dict = entry.model_dump(mode='json')
            f.write(json.dumps(entry_dict) + "\n")
            
    return output_path

@app.command()
def main(
    pdf_path: str = typer.Argument(..., help="Path to the PDF file to process"),
    provider: Provider = typer.Option(Provider.OPENAI, "--provider", "-p", help="AI provider to use (openai or gemini)"),
    output_dir: str = typer.Option("data", "--output-dir", "-o", help="Directory to save the output JSONL file")
):
    """
    Extract COE bidding schedule from a PDF file.
    """
    # Check if PDF exists
    if not os.path.exists(pdf_path):
        print(f"Error: File not found: {pdf_path}")
        return

    # Read the prompt
    try:
        with open("extraction_prompt.md", "r") as f:
            prompt_text = f.read()
    except FileNotFoundError:
        print("Error: extraction_prompt.md not found.")
        return

    # Convert PDF to images
    images = convert_pdf_to_images(pdf_path)
    if not images:
        print("Failed to convert PDF to images.")
        return

    # Extract data
    result = None
    if provider == Provider.OPENAI:
        result = extract_with_openai(images, prompt_text)
    elif provider == Provider.GEMINI:
        result = extract_with_gemini(images, prompt_text)

    # Save result
    if result:
        os.makedirs(output_dir, exist_ok=True)
        save_to_jsonl(result, output_dir)
    else:
        print("Extraction failed.")

if __name__ == "__main__":
    app()

