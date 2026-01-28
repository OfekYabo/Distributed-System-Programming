
import pypdf
import os

pdf_path = "assignment2.pdf"
md_path = "assignment2.md"

try:
    reader = pypdf.PdfReader(pdf_path)
    text_content = []
    
    text_content.append(f"# Assignment 2 Content\n\n")
    
    for i, page in enumerate(reader.pages):
        text = page.extract_text()
        text_content.append(f"## Page {i+1}\n\n")
        text_content.append(text + "\n\n")
        
    with open(md_path, "w", encoding="utf-8") as f:
        f.write("".join(text_content))
        
    print(f"Successfully converted {pdf_path} to {md_path}")

except Exception as e:
    print(f"Error converting PDF: {e}")
