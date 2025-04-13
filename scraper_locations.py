import requests
from bs4 import BeautifulSoup
import psycopg2
import time
import random
import re

# Configuration for PostgreSQL database
DB_CONFIG = {
    'dbname': 'locatie_db',
    'user': 'postgres',
    'password': 'postgres',
    'host': 'localhost',
    'port': 5432
}


class ZorgbalansScraper:
    def __init__(self):
        # Rotate User-Agents to reduce chance of blocking
        self.USER_AGENTS = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        ]

        # Create a requests session for better connection handling
        self.session = requests.Session()

        # Base URL for Zorgbalans locations
        self.BASE_URL = "https://www.zorgbalans.nl"

    def get_headers(self):
        """Generate randomized headers to reduce blocking risk"""
        return {
            'User-Agent': random.choice(self.USER_AGENTS),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1'
        }

    def fetch_page(self, url, max_retries=3):
        """Fetch a page with retry mechanism"""
        for attempt in range(max_retries):
            try:
                response = self.session.get(url, headers=self.get_headers(), timeout=15)
                response.raise_for_status()
                return response
            except requests.RequestException as e:
                print(f"Attempt {attempt + 1} failed: {e}")
                if attempt == max_retries - 1:
                    raise
                time.sleep(2 ** attempt)  # Exponential backoff

    def get_location_links(self, max_pages=10):
        """Retrieve links to all location pages"""
        all_location_links = []

        for page in range(1, max_pages + 1):
            # Construct pagination URL
            if page == 1:
                url = f"{self.BASE_URL}/locaties"
            else:
                url = f"{self.BASE_URL}/locaties/p{page}"

            try:
                response = self.fetch_page(url)
                soup = BeautifulSoup(response.content, 'html.parser')

                # Find all location card links
                location_cards = soup.select('a.card.card--horizontal.card--location')

                if not location_cards:
                    # print(f"No more locations found on page {page}. Stopping.")
                    break

                for card in location_cards:
                    href = card.get('href', '')
                    # Ensure full URL
                    if href.startswith('/'):
                        full_url = f"{self.BASE_URL}{href}"
                    else:
                        full_url = href

                    all_location_links.append(full_url)

                # print(f"Found {len(location_cards)} locations on page {page}")

                # Optional: Add delay between page requests to be kind to the server
                # time.sleep(random.uniform(1, 3))

            except Exception as e:
                print(f"Error fetching page {page}: {e}")
                break

        return all_location_links

    def extract_location_details(self, url):
        """Extract detailed location information from a specific page"""
        try:
            response = self.fetch_page(url)
            soup = BeautifulSoup(response.content, 'html.parser')

            # More robust location detail extraction
            # location_info = {'original_url': url}
            location_info = {}

            # Try multiple selectors to find location name
            name_selectors = [
                'h1.page-header__title',
                '.page-header h1',
                'h1'
            ]
            for selector in name_selectors:
                name_elem = soup.select_one(selector)
                if name_elem:
                    location_info['name'] = name_elem.get_text(strip=True)
                    break

            # Try multiple selectors for address
            address_selectors = [
                '.sidebar__location address',
                '.contact-information address',
                'address'
            ]
            for selector in address_selectors:
                address_elem = soup.select_one(selector)
                if address_elem:
                    address_lines = address_elem.get_text(strip=True, separator="\n").split("\n")

                    if len(address_lines) >= 2:
                        location_info['street'] = address_lines[0]

                        # More robust postal code and city extraction
                        postal_city_parts = address_lines[1].split()
                        if len(postal_city_parts) >= 2:
                            location_info['postal_code'] = " ".join(postal_city_parts[:2])
                            # location_info['city'] = " ".join(postal_city_parts[2:])
                    break

            return location_info

        except Exception as e:
            print(f"Error extracting details from {url}: {e}")
            return None

    def save_to_database(self, locations):
        """Save locations to PostgreSQL database"""
        try:
            conn = psycopg2.connect(**DB_CONFIG)
            cursor = conn.cursor()

            # Create table if not exists
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS werk_locatie (
                id SERIAL PRIMARY KEY,
                naam TEXT,
                straatnaam TEXT,
                postcode TEXT
            )
            """)

            # Insert locations
            for loc in locations:
                cursor.execute("""
                INSERT INTO werk_locatie (naam, straatnaam, postcode) 
                VALUES (%s, %s, %s)
                """, (
                    loc.get('name', 'Onbekend'),
                    loc.get('street', 'Onbekend'),
                    loc.get('postal_code', 'Onbekend')
                ))

            conn.commit()
            # print(f"{len(locations)} locaties opgeslagen.")

        except psycopg2.Error as e:
            print(f"Database error: {e}")

        finally:
            if conn:
                cursor.close()
                conn.close()


def scrape_locations():
    # Scrape all locations
    scraper = ZorgbalansScraper()

    # Get all location links
    location_links = scraper.get_location_links()

    # print(f"Total locations found: {len(location_links)}")

    # Store location details
    location_details = []

    # Fetch details for each location
    for idx, url in enumerate(location_links, 1):
        # print(f"Processing location {idx}/{len(location_links)}: {url}")
        details = scraper.extract_location_details(url)

        if details:
            location_details.append(details)

        # Optional: Add a small delay between requests
        # time.sleep(random.uniform(0.1, 0.2))
        # print(location_details)
        # # Optionally save in batches to prevent memory issues
        # if idx % 20 == 0 or idx == len(location_links):
        #     scraper.save_to_database(location_details)
        #     location_details = []  # Reset for next batch

    return location_details


if __name__ == "__main__":
    scraped_locations = scrape_locations()
    print(len(scraped_locations))
    for scraped_location in scraped_locations:
        print(scraped_location["name"])