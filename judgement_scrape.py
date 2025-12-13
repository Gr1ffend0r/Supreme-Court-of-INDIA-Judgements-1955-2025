import os
import time
import requests
import re
import csv
import argparse
import concurrent.futures
import threading
from bs4 import BeautifulSoup
from urllib.parse import urlparse

class AdvocateKhojScraper:
    MONTHS = {
        1: 'january', 2: 'february', 3: 'march', 4: 'april',
        5: 'may', 6: 'june', 7: 'july', 8: 'august',
        9: 'september', 10: 'october', 11: 'november', 12: 'december'
    }
    
    BASE_URL = "https://www.advocatekhoj.com/library/judgments/index.php"
    
    def __init__(self, output_dir="output", timeout=10, retries=3, delay=1):
        self.output_dir = output_dir
        self.timeout = timeout
        self.retries = retries
        self.delay = delay
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
    def _make_request(self, url):
        for attempt in range(self.retries):
            try:
                response = requests.get(url, headers=self.headers, timeout=self.timeout)
                response.raise_for_status()
                
                # === ADD THIS CHECK ===
                if len(response.content) == 0:
                    print(f"      ⚠️ Empty response received (likely blocked)")
                    # Wait longer before retry
                    if attempt < self.retries - 1:
                        wait_time = 30 * (attempt + 1)  # Wait 30, 60, 90 seconds
                        print(f"      Waiting {wait_time}s before retry...")
                        time.sleep(wait_time)
                        continue  # Try again
                else:
                    # If we got content, break the retry loop (unless it's invalid HTML, checked below)
                    pass
            
                # Also check if it's a valid HTML page
                if '<html' not in response.text[:200].lower() and len(response.text) < 100:
                    print(f"      ⚠️ Suspiciously short response ({len(response.text)} chars)")
                    if attempt < self.retries - 1:
                        time.sleep(5)
                        continue
                    return None
                    
                return response
            

                    
                return response
            
            except requests.RequestException as e:
                if attempt < self.retries - 1:
                    time.sleep(2 ** attempt)
                else:
                    print(f"✗ Failed to fetch {url}: {e}")
                    return None
        return None

    def _setup_year_directory(self, year):
        year_dir = os.path.join(self.output_dir, str(year))
        os.makedirs(year_dir, exist_ok=True)
        return year_dir

    def _init_csv_writer(self, year_dir):
        csv_path = os.path.join(year_dir, "metadata.csv")
        file_exists = os.path.exists(csv_path)
        
        csv_file = open(csv_path, 'a', newline='', encoding='utf-8')
        writer = csv.writer(csv_file)
        
        if not file_exists:
            writer.writerow(['SerialNo', 'Title', 'Date', 'Filename', 'URL'])
            
        return csv_file, writer

    def get_judgment_links(self, year, month):
        month_name = self.MONTHS[month]
        all_links = []
        index_num = 1
        
        print(f"   Searching index pages for {month_name}...")
        
        while True:
            url = f"{self.BASE_URL}?go={year}/{month_name}/indexfiles/index{index_num}.php"
            response = self._make_request(url)
            
            if not response or "Sorry, there is nothing more to show for this month" in response.text:
                break
                
            soup = BeautifulSoup(response.content, 'html.parser')
            found_on_page = 0
            
            for link in soup.find_all('a', onclick=True):
                match = re.search(r"showpage\('(\d+)','(\w+)','(\d+\.php)'", link.get('onclick', ''))
                if match:
                    y, m, filename = match.groups()
                    full_url = f"{self.BASE_URL}?go={y}/{m}/{filename}"
                    if full_url not in all_links:
                        all_links.append(full_url)
                        found_on_page += 1
            
            if found_on_page == 0:
                break
                
            index_num += 1
            time.sleep(self.delay)
            
        return all_links

    def _parse_table(self, table):
        """Converts an HTML table to a text representation"""
        rows = []
        for tr in table.find_all('tr'):
            cells = [td.get_text(separator=' ', strip=True) for td in tr.find_all(['td', 'th'])]
            rows.append(" | ".join(cells))
        
        return "\n" + "\n".join(rows) + "\n"

    def extract_judgment_content(self, url):
        """Extracts text and metadata from judgment page"""
        all_text = []
        page_num = 1
        metadata = {'title': 'Unknown', 'date': 'Unknown'}
        last_content = None

        
        # First page request to get metadata and first chunk of text
        response = self._make_request(url)
        if not response:
            return None, None

        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Extract Metadata from Title
        # Format usually: "Case Name [Date] | ..."
        page_title = soup.title.string if soup.title else ""
        if page_title:
            try:
                # Split by '|' and take the first part
                main_part = page_title.split('|')[0].strip()
                # Check for date in brackets at the end
                date_match = re.search(r'\[(.*?)\]$', main_part)
                if date_match:
                    metadata['date'] = date_match.group(1)
                    metadata['title'] = main_part.replace(f"[{metadata['date']}]", "").strip()
                else:
                    metadata['title'] = main_part
            except Exception:
                pass

        # Extract text loop
        while True:
            curr_url = url if page_num == 1 else f"{url}{'&' if '?' in url else '?'}page={page_num}"
            
            if page_num > 1: # We already have page 1 response
                response = self._make_request(curr_url)
                if not response:
                    break
                soup = BeautifulSoup(response.content, 'html.parser')

            content_div = soup.find("div", {"id": "contentarea"})
            if not content_div:
                break

            # === ADD THIS CHECK ===
            current_content = str(content_div)
            if page_num > 1 and current_content == last_content:
                break
            last_content = current_content


            text_parts = []
            for child in content_div.children:
                if child.name == 'p':
                    text = child.get_text(separator=' ', strip=True)
                    if text: text_parts.append(text)
                elif child.name == 'table':
                    table_text = self._parse_table(child)
                    if table_text.strip(): text_parts.append(table_text)
                elif child.name == 'br' and child.get('clear') == 'all':
                    # End of judgment indicator
                    all_text.append("\n".join(text_parts))
                    return "\n\n".join(all_text), metadata
            
            if text_parts:
                all_text.append("\n".join(text_parts))
            else:
                break # Empty page usually means end
            
            page_num += 1
            time.sleep(self.delay)
            
        return "\n\n".join(all_text), metadata

    def _validate_downloads(self, year, month, links, year_dir):
        """Verifies that all expected files exist"""
        missing = []
        for i, link in enumerate(links, 1):
            match = re.search(r'/(\d+)\.php', link)
            serial_no = match.group(1) if match else f"unknown_{i}"
            filename = f"{year}_{month:02d}_{serial_no}.txt"
            filepath = os.path.join(year_dir, filename)
            
            if not os.path.exists(filepath):
                missing.append(link)
        
        if missing:
            print(f"   ⚠️ WARNING: {len(missing)} files missing after scrape:")
            for m in missing[:5]:
                print(f"      - {m}")
            if len(missing) > 5: print(f"      ... and {len(missing)-5} more")
        else:
            print(f"   ✓ Verification successful: All {len(links)} judgments present.")

    def scrape_month(self, year, month):
        links = self.get_judgment_links(year, month)
        if not links:
            print(f"   No judgments found for {self.MONTHS[month]}")
            return 0

        print(f"   Found {len(links)} judgments. Processing in parallel...")
        
        year_dir = self._setup_year_directory(year)
        csv_file, csv_writer = self._init_csv_writer(year_dir)
        csv_lock = threading.Lock()
        
        total_links = len(links)
        
        def process_link(item):
            i, link = item
            # Get ID
            match = re.search(r'/(\d+)\.php', link)
            serial_no = match.group(1) if match else f"unknown_{i}"
            filename = f"{year}_{month:02d}_{serial_no}.txt"
            filepath = os.path.join(year_dir, filename)
            
            if os.path.exists(filepath):
                print(f"   [{i}/{total_links}] Skipping {filename} (exists)")
                return False

            # print(f"   [{i}/{total_links}] Downloading {filename}...")
            
            data, meta = self.extract_judgment_content(link)
            
            if data:
                with open(filepath, 'w', encoding='utf-8') as f:
                    f.write(data)
                
                with csv_lock:
                    try:
                        csv_writer.writerow([
                            serial_no, 
                            meta['title'], 
                            meta['date'], 
                            filename, 
                            link
                        ])
                        # Force flush to save progress
                        csv_file.flush()
                    except Exception as e:
                        print(f"Error writing to CSV: {e}")
                
                print(f"   [{i}/{total_links}] ✓ Saved {filename}")
                return True
            else:
                print(f"   [{i}/{total_links}] ✗ Empty/Failed {filename}")
                return False

        # Determine workers - if aggressive, use more threads
        max_workers = 10 if self.delay == 0 else 4
        
        success_count = 0
        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                # Reuse the validation logic style index for consistency, assuming links order is stable
                items = list(enumerate(links, 1))
                results = list(executor.map(process_link, items))
                success_count = sum(1 for r in results if r)
                
        finally:
            csv_file.close()

        # Final Validation
        self._validate_downloads(year, month, links, year_dir)
            
        return success_count

    def run(self, start_year, end_year, months=None):
        print("="*60)
        print(f"AdvocateKhoj Scraper | {start_year} -> {end_year}")
        print("="*60)
        
        month_range = months if months else range(1, 13)
        
        # Handle reverse year iteration for consistency with original script
        step = -1 if start_year > end_year else 1
        # Adjust range to include end_year
        stop = end_year - 1 if step == -1 else end_year + 1
        
        for year in range(start_year, stop, step):
            print(f"\nYEAR: {year}")
            print("-" * 20)
            
            for month in month_range:
                try:
                    self.scrape_month(year, month)
                except KeyboardInterrupt:
                    print("\nScraping Interrupted!")
                    return
                except Exception as e:
                    print(f"Error in {year}/{month}: {e}")

def main():
    parser = argparse.ArgumentParser(description="Scrape judgments from AdvocateKhoj")
    parser.add_argument("--start", type=int, default=2025, help="Start year")
    parser.add_argument("--end", type=int, default=1955, help="End year")
    parser.add_argument("--months", type=int, nargs="+", help="Specific months (1-12) to scrape")
    parser.add_argument("--output", type=str, default="/home/hq-asqcd/Documents/Law/output", help="Output directory")
    parser.add_argument("--aggressive", action="store_true", help="Enable aggressive mode (no delay, more retries)")
    
    args = parser.parse_args()
    
    delay = 0 if args.aggressive else 1
    retries = 5 if args.aggressive else 3
    
    scraper = AdvocateKhojScraper(output_dir=args.output, delay=delay, retries=retries)
    scraper.run(args.start, args.end, args.months)

if __name__ == "__main__":
    main()
