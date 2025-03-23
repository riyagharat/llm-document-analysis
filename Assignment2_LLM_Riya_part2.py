# Part 2: Run Ollama and extract entities
import json
import csv
import ollama
from concurrent.futures import ThreadPoolExecutor

# using Ollama LLM extract only new product information from the filing file if it exists
def extract_entities(ticker, filing_time):
    prompt = f"""
    Extract details **ONLY about new product releases** from the SEC Form 8-K filing.

    ### **Strict Guidelines:**
    - **DO NOT** include information about financial earnings, dividends, acquisitions, executive changes, stock buybacks, or any financial statements.
    - **ONLY** extract details if a new product is explicitly announced.

    **Format your response in valid JSON with these fields:**
        - "Company Name": The company filing the 8-K.
        - "Stock Name": The ticker symbol of the company.
        - "Filing Time": The timestamp of the filing.
        - "New Product": The name of the newly announced product, if applicable. Otherwise, leave it blank.
        - "Product Description": A concise summary of the product (max 180 characters). If no product exists, leave this blank.

    Return in JSON format: {{"Company Name": "", "Stock Name": "{ticker}", "Filing Time": "{filing_time}", "New Product": "", "Product Description": ""}}.
    Ensure the response is **VALID JSON ONLY**, without explanations, preambles, or other text.
    """
    try:
        response = ollama.chat("llama3.2", [{"role": "user", "content": prompt}])
        content = response.get("message", {}).get("content", "").strip()

        # Validate and clean JSON -> this is to handle the extra information that Ollama would send back instead of
        # only sending back the JSON object
        try:
            return json.loads(content)
        except json.JSONDecodeError:
            print(f"Invalid JSON received. Attempting repair for {ticker}.")
            content = content.strip("```json").strip("```").strip()
            return json.loads(content)
    except Exception as e:
        print(f"Error extracting entities for {ticker}: {e}")
        return None

# this is the step to process the filings from the filing_data.json file from Part 1 that was the raw data
def process_filings():
    try:
        with open('filing_data.json', 'r', encoding='utf-8') as f:
            filing_data = json.load(f)
    except FileNotFoundError:
        print("No filing data found. Ensure the first script has run successfully.")
        return

    # Open the CSV file for writing, before processing any filings
    # this was done in case I needed to break the code, it would still retain the last run's information
    csv_file = "extracted_entities.csv"
    try:
        with open(csv_file, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f, delimiter=',')
            writer.writerow(["Company Name", "Stock Name", "Filing Time", "New Product", "Product Description"])

            # Using ThreadPoolExecutor for parallelizing the entity extraction
            # This can only be done with processing the filings, because the raw data has already been generated
            with ThreadPoolExecutor(max_workers=5) as executor:
                # Map the extract_entities function to each item in filing_data concurrently
                futures = {executor.submit(extract_entities, item['ticker'], item['filing_time']): item['ticker'] for item in filing_data}

                for future in futures:
                    try:
                        result = future.result()  # Wait for the result of the current future
                        if result and all(result.get(key, "").strip() for key in
                                          ["Company Name", "Stock Name", "Filing Time", "New Product", "Product Description"]):
                            # Write each result to the file as soon as it's available
                            writer.writerow([
                                result.get("Company Name", ""),
                                result.get("Stock Name", ""),
                                result.get("Filing Time", ""),
                                result.get("New Product", ""),
                                result.get("Product Description", "")
                            ])
                            f.flush()  # Flush after each write to ensure data is written to disk immediately
                            print(f"Extracted entities for {futures[future]}")
                        else:
                            print(f"Skipped empty or incomplete data for {futures[future]}")
                    except Exception as e:
                        print(f"Error processing filing for {futures[future]}: {e}")
    except Exception as e:
        print(f"Error while writing to CSV file: {e}")

    print(f"Saved extracted data to {csv_file}")

# runs the main program
if __name__ == "__main__":
    process_filings()