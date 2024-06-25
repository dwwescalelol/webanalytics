import requests
from bs4 import BeautifulSoup


class URLScraper:
    def __init__(self, url):
        self.url = url
        self.html_content = self.fetch_content()
        self.soup = self.parse_content()

    def fetch_content(self):
        """Fetch the HTML content of the URL."""
        try:
            response = requests.get(self.url)
            response.raise_for_status()  # Raise HTTPError for bad responses
            print(f"Successfully fetched content from {self.url}")
            return response.text
        except requests.RequestException as e:
            print(f"Error fetching content from {self.url}: {e}")

    def parse_content(self):
        """Parse the HTML content using BeautifulSoup."""
        if self.html_content:
            print("Successfully parsed HTML content")
            return BeautifulSoup(self.html_content, 'html.parser')
        else:
            print("No HTML content to parse")

    def get_all_links(self):
        """Extract and return all hyperlinks from the page."""
        if self.soup:
            links = [a.get('href') for a in self.soup.find_all('a', href=True)]
            return links
        else:
            print("Soup object is empty, cannot extract links")
            return []

    def get_page_title(self):
        """Extract and return the page title."""
        if self.soup:
            title = self.soup.title.string if self.soup.title else 'No title found'
            return title
        else:
            print("Soup object is empty, cannot extract title")
            return None


# Example usage
if __name__ == "__main__":
    url = "https://www.crazygames.com/tags"
    scraper = URLScraper(url)
    scraper.fetch_content()
    scraper.parse_content()
    print("Page Title:", scraper.get_page_title())
    print("All Links:", scraper.get_all_links())
