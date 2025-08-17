"""
`opencodelists_utils.py`
"""

import re
from datetime import datetime
from io import StringIO

import pandas as pd
import requests
import yaml
from bs4 import BeautifulSoup

from phenolab.utils.definition import Code, Codelist, Definition, DefinitionSource, VocabularyType, vocab_mappings


def load_urls():
    """
    Load OpenCodelists urls from configuration file.
    """
    with open("opencodelists_config.yml", 'r') as f:
        config = yaml.safe_load(f)
    return config['urls']


def scrape_metadata(url):
    """
    Scrape *metadata* from OpenCodelists webpage per url

    Args:
        url:
            url of OpenCodelists page to scrape

    Returns:
        tuple:
            vocabulary, codelist_name, codelist_id, version_id, version_datetime
    """
    response = requests.get(url)
    soup = BeautifulSoup(response.text, "html.parser")

    dl_items = soup.find_all("div", class_="list-group-item")

    for item in dl_items:
        dt = item.find("dt")
        dd = item.find("dd")
        if dt and dd:
            label = dt.get_text(strip=True)
            value = dd.get_text(strip=True)

            if label == "Codelist ID":
                codelist_id = value
            elif label == "Version ID":
                version_id = value
            elif label == "Coding system":
                vocabulary = value

    codelist_name = soup.find("h1").text.strip()

    date_string = soup.find("span", class_="created d-block p-0").text
    date_string = re.sub(r"\s+", " ", date_string).strip()
    version_datetime = datetime.strptime(date_string, "Created: %d %b %Y at %H:%M")

    return vocabulary, codelist_name, codelist_id, version_id, version_datetime


def download_csv_to_dataframe(url):
    """
    Retrieve csv content from OpenCodelists (without downloading to disk).

    Args:
        url:
            url of OpenCodelists page to download csv

    Returns:
        pd.DataFrame:
            Containing csv data
    """
    clean_url = url.rstrip('/')
    csv_url = f"{clean_url}/download.csv"
    response = requests.get(csv_url)
    response.raise_for_status()
    return pd.read_csv(StringIO(response.text))


def process_single_url(url):
    """
    Process a single OpenCodelists url to create PhenoLab Definition objects

    Args:
        url:
            url of OpenCodelists page to process

    Returns:
        list:
            List of definition dictionaries ready for DataFrame creation
    """
    vocabulary, codelist_name, codelist_id, version_id, version_datetime = scrape_metadata(url)

    df_from_csv = download_csv_to_dataframe(url)

    vocabulary_type = vocab_mappings.get(vocabulary, VocabularyType.SNOMED)

    codes = [
        Code(
            code=row.iloc[0],
            code_description=row.iloc[1],
            code_vocabulary=vocabulary_type,
        )
        for _, row in df_from_csv.iterrows()
    ]

    codelist = Codelist(
        codelist_id=codelist_id,
        codelist_name=codelist_name,
        codelist_vocabulary=vocabulary_type,
        codelist_version=version_id,
        codes=codes,
    )

    definition = Definition(
        definition_id=codelist_id,
        definition_name=codelist_name,
        definition_version=version_id,
        definition_source=DefinitionSource.OPENCODELISTS,
        codelists=[codelist],
        version_datetime=version_datetime,
        uploaded_datetime=datetime.now(),
    )

    return definition.aslist

