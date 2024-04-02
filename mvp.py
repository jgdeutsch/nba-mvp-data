import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
import json
import time  # Import the time module

def scrape_table(url, table_id):
    try:
        response = requests.get(url)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
        table = soup.find('table', {'id': table_id})
        if table is None:
            print(f"Table with id '{table_id}' not found in the page. Here's a portion of the page content for debugging:")
            print(soup.prettify()[:2000])  # Print first 2000 characters of the HTML content
            return None
        # Extract header rows
        header_rows = table.thead.find_all('tr')
        categories = [th.text.strip() for th in header_rows[0].find_all('th') if th.text.strip()]
        subcategories = [th.text.strip() for th in header_rows[1].find_all('th')]

        # Combine categories with subcategories for headers
        headers = []
        category_index = 0
        for subcategory in subcategories:
            if subcategory:  # If there's a subcategory, prepend the category
                headers.append(f"{categories[category_index]} - {subcategory}")
            else:  # If no subcategory, just use the category
                headers.append(categories[category_index])
                category_index += 1

        # Extract data rows
        rows = []
        for row in table.tbody.find_all('tr'):
            cells = [cell.text.strip() for cell in row.find_all(['th', 'td'])]
            if cells and len(cells) == len(headers):
                rows.append(cells)

        df = pd.DataFrame(rows, columns=headers)

        return df
    except Exception as e:
        print(f"An error occurred while scraping {url}: {e}")
        return None

urls = [
    'https://www.basketball-reference.com/awards/awards_1968.html#mvp',
    'https://www.basketball-reference.com/awards/awards_1969.html#mvp',
    'https://www.basketball-reference.com/awards/awards_1970.html#mvp',
    'https://www.basketball-reference.com/awards/awards_1971.html#mvp',
    'https://www.basketball-reference.com/awards/awards_1972.html#mvp',
    'https://www.basketball-reference.com/awards/awards_1973.html#mvp',
    'https://www.basketball-reference.com/awards/awards_1974.html#mvp',
    'https://www.basketball-reference.com/awards/awards_1975.html#mvp',
    'https://www.basketball-reference.com/awards/awards_1976.html#mvp'
]

table_id = 'nba_mvp'
data_by_year = {}

for url in urls:
    df = scrape_table(url, table_id)
    if df is not None:
        year_match = re.search(r'awards_(\d{4})', url)
        year = year_match.group(1) if year_match else 'unknown_year'

        data_by_year[year] = {}
        
        for _, row in df.iterrows():
            player_name = row['Voting - Player']  # Adjust the column name if needed
            data_by_year[year][player_name] = row.drop('Voting - Player').to_dict()

    print(f"done scraping {url}\n")

    time.sleep(5)  # Wait for 5 seconds before scraping the next page

json_file_name = "mvp_data_by_year_and_player.json"
with open(json_file_name, 'w') as f:
    json.dump(data_by_year, f, indent=4)

print(f"All MVP data has been structured and saved to {json_file_name}")
