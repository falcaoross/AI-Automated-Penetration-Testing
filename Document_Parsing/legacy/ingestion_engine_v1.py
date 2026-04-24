import fitz
import pytesseract
import cv2
import camelot
import pandas as pd
import json, os
from pathlib import Path
from docx import Document
import pdfplumber

BASE_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = BASE_DIR / "output"
IMAGES_DIR = OUTPUT_DIR / "extracted_images"

# ---- Helper Functions ----
def extract_pdf_content(path):
    print(f"[PDF] Processing: {path}")
    result = {"text": [], "ocr_text": [], "tables": []}
    
    # Create output directory structure
    IMAGES_DIR.mkdir(parents=True, exist_ok=True)
    
    # Get document name without extension for organizing images
    doc_name = os.path.splitext(os.path.basename(path))[0]
    doc_img_dir = IMAGES_DIR / doc_name
    doc_img_dir.mkdir(parents=True, exist_ok=True)
    
    doc = fitz.open(path)
    for pno, page in enumerate(doc, 1):
        result["text"].append({"page": pno, "content": page.get_text("text")})
        for i, img in enumerate(page.get_images(full=True)):
            xref = img[0]
            pix = fitz.Pixmap(doc, xref)
            img_path = doc_img_dir / f"page{pno}_img{i+1}.png"
            (pix if pix.n < 5 else fitz.Pixmap(fitz.csRGB, pix)).save(str(img_path))
            img_cv = cv2.imread(str(img_path))
            gray = cv2.cvtColor(img_cv, cv2.COLOR_BGR2GRAY)
            text = pytesseract.image_to_string(gray)
            result["ocr_text"].append({"page": pno, "image": str(img_path), "content": text})
    doc.close()

    try:
        tables = camelot.read_pdf(path, pages="all", flavor="stream")
        for i, t in enumerate(tables):
            result["tables"].append({"page": t.page, "data": t.df.to_dict(orient="records")})
    except Exception as e:
        print("Table extraction failed:", e)
    return result


def extract_docx(path):
    print(f"[DOCX] Processing: {path}")
    doc = Document(path)
    text = "\n".join([p.text for p in doc.paragraphs])
    return {"text": text}


def extract_txt(path):
    print(f"[TXT] Processing: {path}")
    with open(path, "r", encoding="utf-8") as f:
        text = f.read()
    return {"text": text}


def extract_xlsx(path):
    print(f"[XLSX] Processing: {path}")
    xls = pd.ExcelFile(path)
    sheets = {sheet: pd.read_excel(xls, sheet).to_dict(orient="records") for sheet in xls.sheet_names}
    return {"tables": sheets}


def extract_image(path):
    print(f"[IMG] Processing: {path}")
    img = cv2.imread(path)
    gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
    text = pytesseract.image_to_string(gray)
    return {"ocr_text": text}


# ---- Master Function ----
def process_file(file_path):
    ext = os.path.splitext(file_path)[1].lower()
    if ext in [".pdf"]:
        return extract_pdf_content(file_path)
    elif ext in [".docx"]:
        return extract_docx(file_path)
    elif ext in [".txt"]:
        return extract_txt(file_path)
    elif ext in [".xlsx"]:
        return extract_xlsx(file_path)
    elif ext in [".png", ".jpg", ".jpeg"]:
        return extract_image(file_path)
    else:
        raise ValueError("Unsupported file format.")


# ---- Runner ----
if __name__ == "__main__":
    input_path = input("Enter file path (PDF/DOCX/TXT/XLSX/IMG): ").strip()
    data = process_file(input_path)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    output_file = OUTPUT_DIR / "structured_output.json"
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"✅ Data saved to {output_file}")
