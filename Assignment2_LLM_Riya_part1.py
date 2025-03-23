# Part 1: Fetch and Store 8-K Filings
import json
import time
import requests
from bs4 import BeautifulSoup

# headers to pass to the endpoints,
HEADERS = {
    "User-Agent": "Riya Poon riya.gharat@yahoo.com",
    "Accept-Encoding": "gzip, deflate",
    "Host": "www.sec.gov",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Connection": "keep-alive",
}

# fetch s&p 500 tickers from wikipedia's page
def fetch_sp500_tickers():
    url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    table = soup.find('table', {'id': 'constituents'})
    return [row.find_all('td')[0].text.strip() for row in table.find_all('tr')[1:]]

# use the given company_tickers file to get the CIK mapping
def get_cik_mapping():
    with open("company_tickers.json", "r") as f:
        cik_data = json.load(f)
    return {item["ticker"].upper(): str(item["cik_str"]).zfill(10) for item in cik_data.values()}

# for each CIK get the last 20 filings
def get_8k_filings(cik, count=20):
    url = f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK={cik}&type=8-K&count={count}&output=atom"
    try:
        response = requests.get(url, headers=HEADERS, timeout=60)
        response.raise_for_status()
        # sleep is to allow for rate limiting
        time.sleep(2)
        soup = BeautifulSoup(response.text, "xml")
        entries = soup.find_all("entry")
        return [(entry.link["href"], entry.updated.text) for entry in entries[:count]]
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data for CIK {cik}: {e}")
        return []

# extract the entire text from the filing using BeautifulSoup
def extract_filing_text(url, ticker):
    try:
        response = requests.get(url, headers=HEADERS, timeout=30)
        response.raise_for_status()
        # sleep is to allow for rate limiting
        time.sleep(5)
        soup = BeautifulSoup(response.content, "html.parser")
        rows = soup.find_all("tr")

        for row in rows:
            columns = row.find_all("td")
            for col in columns:
                links = col.find_all("a", href=True)
                for link in links:
                    if link["href"].endswith(".htm") and (ticker.lower() in link["href"] or '8k' in link["href"]):
                        href_parts = link["href"].split("/")
                        filing_url = f"https://www.sec.gov/Archives/edgar/data/{href_parts[-3]}/{href_parts[-2]}/{href_parts[-1]}"
                        response = requests.get(filing_url, headers=HEADERS, timeout=60)
                        # sleep is to allow for rate limiting
                        time.sleep(2)
                        filing_soup = BeautifulSoup(response.content, "html.parser")
                        return filing_soup.get_text(strip=True)
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data for ticker {ticker}: {e}")
        return ""


def main():
    tickers = fetch_sp500_tickers()
    cik_mapping = get_cik_mapping()
    filing_data = []

# for each ticker that is in S&P 500
    for ticker in tickers:
        if ticker not in cik_mapping:
            continue
        cik = cik_mapping[ticker]
        filings = get_8k_filings(cik)
        # error handling for if there are no filings
        if not filings:
            print(f"No filings fetched for CIK {cik}. Skipping...")
            continue

        # for each filing_url
        for filing_url, filing_time in filings:
            text = extract_filing_text(filing_url, ticker)
            if text:
                filing_data.append({"ticker": ticker, "filing_time": filing_time, "text": text})
                print(f"Fetched 8-K filing for {ticker}")

    # Save to JSON -> this is the raw data of each ticker's filing data
    with open("filing_data.json", "w", encoding="utf-8") as f:
        json.dump(filing_data, f, ensure_ascii=False, indent=4)
    print("Saved filing data to filing_data.json")

# start the program
if __name__ == "__main__":
    main()
