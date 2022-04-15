from pathlib import Path
from typing import Dict, List, Optional

import bs4
import pandas as pd
import requests
from requests.models import HTTPError


class NCSLScraper(object):
    """Scrape NCSL website for wind energy permitting regulations."""
    SOURCE_URL = "https://www.ncsl.org/research/energy/state-wind-energy-siting.aspx"

    def __init__(self) -> None:
        self.soup: Optional[bs4.BeautifulSoup] = None
        return

    def _get_page(self) -> None:
        """submit GET request and pass into beautifulsoup."""
        page = requests.get(NCSLScraper.SOURCE_URL)
        if page.status_code != 200:
            raise HTTPError(
                f'Bad response from NCSL. Status code: {page.status_code}')
        self.soup = bs4.BeautifulSoup(page.content, 'html.parser')
        return

    def _parse_state_div(self, div: bs4.element.Tag, ) -> Dict[str, str]:
        """Parse html <div>s corresponding to each state and extract relevant information.

        Args:
            div (bs4.element.Tag): html <div> corresponding to each state

        Returns:
            Dict[str, str]: dict with keys state, permitting_type, description, and link
        """
        try:
            # State names have H2 headers
            state_name = div.select('h2')[0].text.strip()
        except IndexError:
            # second choice due to no spaces between words (ex. 'NewYork')
            state_name = div.attrs['id']
        tags = div.find_all('p')

        for tag in tags:
            # state flag image
            if tag.contents[0].text == '':
                continue
            # Statute link
            elif tag.contents[0].text == 'Statute':
                try:
                    statute_link = tag.select('a')[0].attrs['href']
                except IndexError:  # N/A values have no links
                    statute_link = ''
            # statute summary
            elif tag.contents[0].text == 'Summary':
                description = tag.contents[2].strip()

            # statute type
            elif tag.contents[0].text == 'Authority':
                permitting_type = tag.contents[2].strip()
            else:
                raise ValueError(
                    f"Unexpected content title: {tag.contents[0].text}")

        output = {
            'state': state_name,
            'permitting_type': permitting_type,
            'description': description,
            'link': statute_link,
        }
        return output

    def parse_page(self) -> pd.DataFrame:
        """Get and parse NCSL wind energy permitting page.

        Returns:
            pd.DataFrame: dataframe of raw scraped data.
        """
        if self.soup is None:
            self._get_page()

        scraped: Dict[str, List[str]] = {
            'state': [],
            'permitting_type': [],
            'description': [],
            'link': [],
        }
        for state_div in self.soup.find_all('div',  # type: ignore
                                            class_="panel notshowing"):
            state_entry = self._parse_state_div(state_div)
            for attr, value in state_entry.items():
                scraped[attr].append(value)

        output = pd.DataFrame(scraped).sort_values(
            'state').reset_index(drop=True)
        return output

    def scrape_and_save_to_disk(self, destination_path: Path) -> None:
        """Get NCSL winder energy permitting data, parse it, and save results to disk.

        Args:
            destination_path (Path): filepath to write csv to
        """
        scraped = self.parse_page()
        scraped.to_csv(destination_path, index=False)
        return


def extract(csv) -> Dict[str, pd.DataFrame]:
    # TODO: use datastore
    out = pd.read_csv(csv)
    # set categorical dtypes in transform stage, after cleaning up categories.
    return {'ncsl_state_permitting': out}
