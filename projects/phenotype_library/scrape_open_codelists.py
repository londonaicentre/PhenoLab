import re
from datetime import datetime

import requests
from bs4 import BeautifulSoup


def return_version_id_from_open_codelist_url(url: str) -> tuple[str, str, str, str, str]:
    response = requests.get(url)
    soup = BeautifulSoup(response.text, "html.parser")

    vocabulary, _, _, codelist_id, _, version_id = [
        e.text for e in soup.find_all("dd", class_="pb-2 border-bottom")
    ]

    codelist_name = soup.find("h1").text

    date_string = soup.find("span", class_="created d-block p-0").text
    date_string = re.sub(r"\s+", " ", date_string).strip()
    version_datetime = datetime.strptime(date_string, "Created: %d %b %Y at %H:%M")
    # print(version_datetime)

    # csv_link = (
    #     "https://www.opencodelists.org"
    #     + [link.get("href") for link in soup.find_all("a")
    #     if "download.csv" in link.get("href")][0]
    # )
    #  print(csv_link)
    # unfortunately trying to open the csv with pandas (via requests) gives a 403 error

    return vocabulary, codelist_name, codelist_id, version_id, version_datetime


if __name__ == "__main__":
    vocabulary, codelist_name, codelist_id, version_id = return_version_id_from_open_codelist_url(
        "https://www.opencodelists.org/codelist/nhsd-primary-care-domain-refsets/abpm_cod/20241205/"
    )
    return_version_id_from_open_codelist_url(
        "https://www.opencodelists.org/codelist/opensafely/hypertension-snomed/2020-04-28/"
    )
