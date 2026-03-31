import pdfplumber

path = "02112026StevensPoint.pdf"

with pdfplumber.open(path) as pdf:
    print(f"Page count: {len(pdf.pages)}")
    for i, page in enumerate(pdf.pages, start=1):
        text = page.extract_text()
        print(f"\n--- Page {i} ---\n")
        print(text or "[no text extracted]")