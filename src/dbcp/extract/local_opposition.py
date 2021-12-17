from typing import Dict, List, Optional
from pathlib import Path

import pandas as pd
import docx

from pudl.metadata.enums import US_STATES

class ColumbiaDocxParser(object):
    POSSIBLE_STATES = set(US_STATES.values())
    POSSIBLE_HEADERS = {'Local Laws/Ordinances', 'Contested Projects', 'State Policy'}
    FIRST_STATE = 'Alabama'
    NULL_ORDINANCE = {'No ordinances were found at this time.', 'No local ordinances were found at this time.'}
    NULL_PROJECT = {'No contested projects were found at this time.',}

    def __init__(self) -> None:
        #TODO: refactor to use datastore
        self.current_state = ''
        self.current_header = ''
        self.doc: Optional[docx.Document] = None
        
        self.state_policy_dict: Dict[str, List[str]] = {'state': [], 'policy': []}
        self.local_ordinance_dict: Dict[str, List[str]] = {'state': [], 'locality': [], 'ordinance': []}
        self.contested_projects_dict: Dict[str, List[str]] = {'state': [], 'project_name': [], 'description': []}
    
    def load_docx(self, source_path = Path('/app/input/RELDI report updated 9.10.21 (1).docx')) -> None:
        self.doc = docx.Document(source_path)

    def _remove_intro(self, paragraphs: List[docx.text.paragraph.Paragraph]) -> List[docx.text.paragraph.Paragraph]:
        for idx, paragraph in enumerate(paragraphs):
            if paragraph.text.strip() == ColumbiaDocxParser.FIRST_STATE:
                return paragraphs[idx:]
        raise ValueError('Could not find starting state')


    def _parse_values(self, text: str) -> None:
        if self.current_header == 'State Policy':
            # no null check required. This section is simply missing if null.
            self.state_policy_dict['state'].append(self.current_state)
            self.state_policy_dict['policy'].append(text)
            return

        elif self.current_header == 'Local Laws/Ordinances':
            if text in ColumbiaDocxParser.NULL_ORDINANCE:
                return
            locality, ordinance = text.split(':', maxsplit=1)
            self.local_ordinance_dict['state'].append(self.current_state)
            self.local_ordinance_dict['locality'].append(locality)
            self.local_ordinance_dict['ordinance'].append(ordinance.strip())
            return

        elif self.current_header == 'Contested Projects':
            if text in ColumbiaDocxParser.NULL_PROJECT:
                return
            try:
                name, description = text.split(':', maxsplit=1)
            except ValueError: # no split
                name = 'unknown'
                description = text
            self.contested_projects_dict['state'].append(self.current_state)
            self.contested_projects_dict['project_name'].append(name)
            self.contested_projects_dict['description'].append(description.strip())
            return
        # else: not required. Set membership test occurs prior to this function
        

    def extract(self) -> Dict[str, pd.DataFrame]:
        """Parse the text of the Columbia Local Opposition docx file into tabular dataframes.

        The document is essentially a hierarchical data structure with two nested layers.
        The parser iterates through each line, testing which level of hierarchy it is in
        and then extracting values into a dictionary (later converted to pd.DataFrame)

        Returns:
            Dict[str, pd.DataFrame]: return dataframes with keys 'state_policy', 'local_ordinance', and 'contested_project'
        """
        if self.doc is None:
            raise ValueError('Use the .load_docx() method to load the document.')

        paragraphs = self._remove_intro(self.doc.paragraphs)

        for paragraph in paragraphs:
            if paragraph.text == '': # skip blank lines
                continue
            elif paragraph.style.name == 'Heading 1': # states
                self.current_state = paragraph.text.strip()
                assert self.current_state in ColumbiaDocxParser.POSSIBLE_STATES, f'Parsing error. Check state: {self.current_state}'
                self.current_header = '' # a new state marks a new hierarchy, so reset cache
            elif paragraph.style.name == 'Heading 2': # value type
                self.current_header = paragraph.text.strip()
                assert self.current_header in ColumbiaDocxParser.POSSIBLE_HEADERS, f'Parsing error. Check header: {self.current_header}'
            elif paragraph.style.name == 'Normal': # values
                self._parse_values(paragraph.text.strip())
        
        return {
            'state_policy': pd.DataFrame(self.state_policy_dict),
            'local_ordinance': pd.DataFrame(self.local_ordinance_dict),
            'contested_project': pd.DataFrame(self.contested_projects_dict)
        }
