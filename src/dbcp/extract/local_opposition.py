"""Extraction logic for Columbia Local Opposition dataset."""
from pathlib import Path
from typing import Dict, List, Optional

import docx
import pandas as pd

from pudl.metadata.enums import US_STATES


class ColumbiaDocxParser(object):
    """Parser for the Columbia Local Opposition .docx file."""

    POSSIBLE_STATES = set(US_STATES.values())
    POSSIBLE_HEADERS = {'Local Laws/Ordinances',
                        'Contested Projects', 'State Policy'}
    FIRST_STATE = 'Alabama'
    NULL_ORDINANCE = {'No ordinances were found at this time.',
                      'No local ordinances were found at this time.'}
    NULL_PROJECT = {'No contested projects were found at this time.', }

    def __init__(self) -> None:
        """Create ColumbiaDocxParser object to parse docx data."""
        self.current_state = ''
        self.current_header = ''
        self.doc: Optional[docx.Document] = None
        self.previous_locality = ''
        self.previous_ordinance = ''

        self.state_policy_dict: Dict[str, List[str]] = {
            'state': [], 'policy': []}
        self.local_ordinance_dict: Dict[str, List[str]] = {
            'state': [], 'locality': [], 'ordinance': []}
        self.contested_projects_dict: Dict[str, List[str]] = {
            'state': [], 'project_name': [], 'description': []}

    def load_docx(self, source_path=Path('/app/data/raw/RELDI report updated 9.10.21 (1).docx')) -> None:
        """Read the .docx file with python-docx.

        Args:
            source_path (Path, optional): path to .docx file. Defaults to Path('/app/data/raw/RELDI report updated 9.10.21 (1).docx').
        """
        self.doc = docx.Document(source_path)

    def _remove_intro(self, paragraphs: List[docx.text.paragraph.Paragraph]) -> List[docx.text.paragraph.Paragraph]:
        """Skip over The title page, table of contents, intro, etc that contain no data.

        Args:
            paragraphs (List[docx.text.paragraph.Paragraph]): the list of paragraphs

        Raises:
            ValueError: if the marker for the start of the data cannot be found.

        Returns:
            List[docx.text.paragraph.Paragraph]: subset of paragraphs, from the first data line to the end.
        """
        for idx, paragraph in enumerate(paragraphs):
            if paragraph.text.strip() == ColumbiaDocxParser.FIRST_STATE:
                return paragraphs[idx:]
        raise ValueError('Could not find starting state')

    def _parse_values(self, text: str) -> None:
        """Parse and assign values to the correct dataset based on the current hierarchical headings.

        Args:
            text (str): the paragraph text content
        """
        if self.current_header == 'State Policy':
            # no null check required. This section is simply missing if null.
            self.state_policy_dict['state'].append(self.current_state)
            self.state_policy_dict['policy'].append(text)
            return

        elif self.current_header == 'Local Laws/Ordinances':
            if text in ColumbiaDocxParser.NULL_ORDINANCE:
                return
            locality, ordinance = text.split(':', maxsplit=1)
            # Brownsville and Benbrook TX have an extra level of hierarchy.
            if locality in {'Wind', 'Solar'}:
                locality = self.previous_locality
                ordinance = self.previous_ordinance + ordinance
            else:
                self.previous_locality = locality
                self.previous_ordinance = ordinance

            self.local_ordinance_dict['state'].append(self.current_state)
            self.local_ordinance_dict['locality'].append(locality)
            self.local_ordinance_dict['ordinance'].append(ordinance.strip())
            return

        elif self.current_header == 'Contested Projects':
            if text in ColumbiaDocxParser.NULL_PROJECT:
                return
            try:
                name, description = text.split(':', maxsplit=1)
            except ValueError:  # no split
                name = ''
                description = text
            self.contested_projects_dict['state'].append(self.current_state)
            self.contested_projects_dict['project_name'].append(name)
            self.contested_projects_dict['description'].append(
                description.strip())
            return
        else:
            raise ValueError(f'Unexpected header: {self.current_header}')

    def extract(self) -> Dict[str, pd.DataFrame]:
        """Parse the text of the Columbia Local Opposition docx file into tabular dataframes.

        The document is essentially a hierarchical data structure with two nested layers.
        The parser iterates through each line, testing which level of hierarchy it is in
        and then extracting values into a dictionary (later converted to pd.DataFrame)

        Returns:
            Dict[str, pd.DataFrame]: return dataframes with keys 'state_policy', 'local_ordinance', and 'contested_project'
        """
        if self.doc is None:
            raise ValueError(
                'Use the .load_docx() method to load the document.')

        paragraphs = self._remove_intro(self.doc.paragraphs)

        for paragraph in paragraphs:
            if paragraph.text == '':  # skip blank lines
                continue
            elif paragraph.style.name == 'Heading 1':  # states
                self.current_state = paragraph.text.strip()
                assert self.current_state in ColumbiaDocxParser.POSSIBLE_STATES, f'Unexepected state: {self.current_state}'
                self.current_header = ''  # a new state marks a new hierarchy, so reset cache
            elif paragraph.style.name == 'Heading 2':  # value type
                self.current_header = paragraph.text.strip()
                assert self.current_header in ColumbiaDocxParser.POSSIBLE_HEADERS, f'Unexpected header: {self.current_header}'
            elif paragraph.style.name in {'Normal', 'List Paragraph', 'Normal1'}:  # values
                # This hardcoded style checking is slightly less brittle than it seems.
                # Any mis-styled states or headers would be obvious from the table of contents.
                # A future improvement could be to ensure that is true, but I think this data is static
                # so will defer that work.
                self._parse_values(paragraph.text.strip())
            else:
                raise ValueError(
                    f'Unexpected paragraph style: {paragraph.style.name}')

        output = {
            'state_policy': pd.DataFrame(self.state_policy_dict),
            'local_ordinance': pd.DataFrame(self.local_ordinance_dict),
            'contested_project': pd.DataFrame(self.contested_projects_dict)
        }
        # manual correction for Brownsville and Benbrook TX, which have an extra level of hierarchy
        # that produces an extra empty row
        query_str = "state == 'Texas' and (locality == 'Brownsville' or locality == 'Benbrook')"
        subset = output['local_ordinance'].query(query_str).reset_index()
        if not subset.empty:
            if subset['ordinance'].iat[0] != "Enacted July 9, 2020:":
                raise ValueError(
                    'Data has changed and Brownsville correction is no longer valid')
        indices_to_delete = pd.Index(subset.groupby(
            ['state', 'locality'], as_index=False).first()['index'])
        output['local_ordinance'].drop(index=indices_to_delete, inplace=True)
        return output
