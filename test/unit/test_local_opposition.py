"""Test Local opposition ETL code."""
from collections import namedtuple

import pandas as pd
import pytest

from dbcp.extract.local_opposition import ColumbiaDocxParser

MockStyle = namedtuple("MockStyle", "name")
MockParagraph = namedtuple(
    "MockParagraph",
    [
        "text",
        "style",
    ],
)
MockDocument = namedtuple("MockDocument", ["paragraphs"])


def mock_parser_factory(mock_document: MockDocument):
    """Create a mock ColumbiaDocxParser for testing."""
    output = ColumbiaDocxParser()
    output.doc = mock_document
    return output


def test__remove_intro():
    """Test ColumbiaDocxParser ability to remove document intros."""
    input_list_of_paragraphs = [
        MockParagraph(text="some title", style=MockStyle(name="Heading 1")),
        MockParagraph(text="some table of contents", style=MockStyle(name="Heading 2")),
        MockParagraph(text="some intro", style=MockStyle(name="Normal")),
        MockParagraph(
            text=ColumbiaDocxParser.FIRST_STATE + " extra",
            style=MockStyle(name="Heading 1"),
        ),
        MockParagraph(
            text=ColumbiaDocxParser.FIRST_STATE, style=MockStyle(name="Heading 1")
        ),
        MockParagraph(text="line to keep", style=MockStyle(name="Heading 1")),
    ]
    expected = [  # the lines to keep
        MockParagraph(
            text=ColumbiaDocxParser.FIRST_STATE, style=MockStyle(name="Heading 1")
        ),
        MockParagraph(text="line to keep", style=MockStyle(name="Heading 1")),
    ]
    parser = ColumbiaDocxParser()
    actual = parser._remove_intro(input_list_of_paragraphs)
    assert actual == expected
    bad_input = input_list_of_paragraphs[:2]  # no FIRST_STATE
    with pytest.raises(ValueError):
        parser._remove_intro(bad_input)


def test_extract():  # integration test
    """Test docx extraction code."""
    data = MockDocument(
        [
            MockParagraph(text="some intro", style=MockStyle(name="Normal")),
            MockParagraph(text="Alabama", style=MockStyle(name="Heading 1")),
            MockParagraph(
                text="State-Level Restrictions", style=MockStyle(name="Heading 2")
            ),
            MockParagraph(text="Very Important Policy", style=MockStyle(name="Normal")),
            MockParagraph(text="Local Restrictions", style=MockStyle(name="Heading 2")),
            MockParagraph(
                text="Lovely County: Important Ordinance",
                style=MockStyle(name="Normal"),
            ),
            MockParagraph(text="Contested Projects", style=MockStyle(name="Heading 2")),
            MockParagraph(
                text="Amazing Project: Sad Story", style=MockStyle(name="Normal")
            ),
        ]
    )
    parser = mock_parser_factory(data)
    expected = {
        "state_policy": pd.DataFrame(
            {"state": ["Alabama"], "policy": ["Very Important Policy"]}
        ),
        "state_notes": pd.DataFrame({"state": [], "notes": []}),
        "local_ordinance": pd.DataFrame(
            {
                "state": ["Alabama"],
                "locality": ["Lovely County"],
                "ordinance_text": ["Important Ordinance"],
            }
        ),
        "contested_project": pd.DataFrame(
            {
                "state": ["Alabama"],
                "project_name": ["Amazing Project"],
                "description": ["Sad Story"],
            }
        ),
    }
    actual = parser.extract()
    for key, df in actual.items():
        pd.testing.assert_frame_equal(df, expected[key])
