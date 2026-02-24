"""Extraction logic for Columbia Local Opposition dataset.

This dataset is a .docx file with a hierarchical structure.  The hierarchy is denoted by
formatting details (paragraph level, font, etc), but is surprisingly consistent. It is
infrequently updated by a research group at Columbia University.
"""

import docx
import pandas as pd

from dbcp.constants import DATA_DIR, US_STATES


class ColumbiaDocxParser:
    """Parser for the Columbia Local Opposition .docx file."""

    POSSIBLE_STATES = US_STATES
    POSSIBLE_HEADERS = {
        "Local Restrictions",
        "Contested Projects",
        "State-Level Restrictions",
    }
    POSSIBLE_SUBHEADINGS = {
        "Existing Entries (Updated)",
        "New Entries (Pre-March 2022 Developments)",
        "New Entries (Post-March 2022 Developments)",
        "New Restrictions (Post-March 2022 Developments)",
        "New Entries (Post-March 2022 Updates)",
    }
    FIRST_STATE = "Alabama"
    NULL_STATE_POLICY = {
        "No restrictive state laws, regulations, or policies were found at this time."
    }
    NULL_ORDINANCE = {
        "No restrictive local ordinances, regulations, or policies were found at this time.",
        "No local ordinances were found at this time.",
    }
    NULL_PROJECT = {
        "No contested projects were found at this time.",
    }

    def __init__(self) -> None:
        """Create ColumbiaDocxParser object to parse docx data."""
        self.current_state = ""
        self.current_header = ""
        self.doc: docx.Document | None = None
        self.previous_locality = ""
        self.previous_ordinance = ""

        self.state_policy_dict: dict[str, list[str]] = {"state": [], "policy": []}
        self.state_notes_dict: dict[str, list[str]] = {"state": [], "notes": []}
        self.local_ordinance_dict: dict[str, list[str]] = {
            "state": [],
            "locality": [],
            "ordinance_text": [],
        }
        self.contested_projects_dict: dict[str, list[str]] = {
            "state": [],
            "project_name": [],
            "description": [],
        }

    def load_docx(
        self, source_path=DATA_DIR / "raw/RELDI report updated 9.10.21 (1).docx"
    ) -> None:
        """Read the .docx file with python-docx.

        Args:
            source_path (Path, optional): path to .docx file. Defaults to Path('/app/data/raw/RELDI report updated 9.10.21 (1).docx').

        """
        self.doc = docx.Document(source_path)

    def _remove_intro(
        self, paragraphs: list[docx.text.paragraph.Paragraph]
    ) -> list[docx.text.paragraph.Paragraph]:
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
        raise ValueError("Could not find starting state")

    def _parse_values(self, text: str) -> None:  # noqa: C901
        """Parse and assign values to the correct dataset based on the current hierarchical headings.

        Args:
            text (str): the paragraph text content

        """
        if self.current_header == "":  # state level notes
            # no null check required. This section is simply missing if null.
            self.state_notes_dict["state"].append(self.current_state)
            self.state_notes_dict["notes"].append(text)
            return
        if self.current_header == "State-Level Restrictions":
            if text in ColumbiaDocxParser.NULL_STATE_POLICY:
                return
            self.state_policy_dict["state"].append(self.current_state)
            self.state_policy_dict["policy"].append(text)
            return

        if self.current_header == "Local Restrictions":
            if text in ColumbiaDocxParser.NULL_ORDINANCE:
                return
            locality, ordinance = text.split(":", maxsplit=1)
            # Brownsville and Benbrook TX have an extra level of hierarchy.
            if locality in {"Wind", "Solar"}:
                locality = self.previous_locality
                ordinance = self.previous_ordinance + ordinance
            else:
                self.previous_locality = locality
                self.previous_ordinance = ordinance

            self.local_ordinance_dict["state"].append(self.current_state)
            self.local_ordinance_dict["locality"].append(locality)
            self.local_ordinance_dict["ordinance_text"].append(ordinance.strip())
            return

        if self.current_header == "Contested Projects":
            if text in ColumbiaDocxParser.NULL_PROJECT:
                return
            try:
                name, description = text.split(":", maxsplit=1)
            except ValueError:  # no split
                name = ""
                description = text
            self.contested_projects_dict["state"].append(self.current_state)
            self.contested_projects_dict["project_name"].append(name)
            self.contested_projects_dict["description"].append(description.strip())
            return
        raise ValueError(
            f"Unexpected header in {self.current_state}: {self.current_header}"
        )

    def extract(self) -> dict[str, pd.DataFrame]:
        """Parse the text of the Columbia Local Opposition docx file into tabular dataframes.

        The document is essentially a hierarchical data structure with two nested layers.
        The parser iterates through each line, testing which level of hierarchy it is in
        and then extracting values into a dictionary (later converted to pd.DataFrame)

        Returns:
            Dict[str, pd.DataFrame]: return dataframes with keys 'state_policy', 'local_ordinance', and 'contested_project'

        """
        if self.doc is None:
            raise ValueError("Use the .load_docx() method to load the document.")

        paragraphs = self._remove_intro(self.doc.paragraphs)

        for paragraph in paragraphs:
            if paragraph.text == "":  # skip blank lines
                continue
            if paragraph.style.name == "Heading 1":  # states
                self.current_state = paragraph.text.strip()
                assert self.current_state in ColumbiaDocxParser.POSSIBLE_STATES, (
                    f"Unexepected state: {self.current_state}"
                )
                self.current_header = (
                    ""  # a new state marks a new hierarchy, so reset cache
                )
            elif paragraph.style.name == "Heading 2":  # value type
                self.current_header = paragraph.text.strip()
                assert self.current_header in ColumbiaDocxParser.POSSIBLE_HEADERS, (
                    f"Unexpected header in {self.current_state}: {self.current_header}"
                )
            elif (
                paragraph.style.name == "Heading 3"
            ):  # nearly meaningless subheading. skip.
                assert (
                    paragraph.text.strip() in ColumbiaDocxParser.POSSIBLE_SUBHEADINGS
                ), (
                    f"Unexpected subheading in {self.current_state}: {paragraph.text.strip()}"
                )
                continue
            elif paragraph.style.name in {
                "Normal",
                "List Paragraph",
                "Normal1",
            }:  # values
                # This hardcoded style checking is brittle. If the docx changes, this will break.
                self._parse_values(paragraph.text.strip())
            else:
                raise ValueError(
                    f"Unexpected paragraph style in {self.current_state}: {paragraph.style.name}"
                )

        output = {
            "state_policy": pd.DataFrame(self.state_policy_dict),
            "state_notes": pd.DataFrame(self.state_notes_dict),
            "local_ordinance": pd.DataFrame(self.local_ordinance_dict),
            "contested_project": pd.DataFrame(self.contested_projects_dict),
        }

        return output
