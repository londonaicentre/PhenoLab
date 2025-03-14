import datetime
import hashlib
import json
import os
from dataclasses import dataclass, field
from typing import Dict, List

"""
## data_utils.py

This contains classes for:
- Definition, Codelist, Code
- Because of the need to update versions, this is distinct from the one used in definition_library
Contains methods:
- to add and remove Codes from Definitions
- to create Definition object and save as json
- to load json into Definition object
To do:
- Pull in concepts from different types (e.g. meds) and enum DEFINITION_TYPE
"""

@dataclass
class Code:
    """
    Single EHR medical code representing a single concept, from a standard vocabulary
    """
    code: str
    code_description: str
    vocabulary: str


@dataclass
class Codelist:
    """
    Collection of codes from the same vocabulary that define a single broad clinical concept
    """
    codelist_name: str
    codes: List[Code] = field(default_factory=list)
    codelist_id: str = field(default=None)
    codelist_version: str = field(default=None)
    _modified: bool = field(default=True) # flag updates

    def __post_init__(self):
        """
        This inits a codelist ID and version *if not provided on a load*.
        Creates 8-character hash ID based on codelist name and timestamp at init
        Initialises version string if not provided (see _update_version).
        """
        # ceate ID only if not provided (i.e. new codelist)
        if self.codelist_id is None:
            timestamp = datetime.datetime.now()
            content = f"{self.codelist_name}_{timestamp.isoformat()}"
            self.codelist_id = hashlib.md5(content.encode()).hexdigest()[:8]

        # create initial version if not provided
        if self.codelist_version is None:
            self._update_version()

    def _update_version(self):
        """
        Update the codelist version, and sets modified flag to False
        Used to update version when the codelist has been updated
        """
        timestamp = datetime.datetime.now()
        self.codelist_version = f"{self.codelist_name}_{timestamp.strftime('%Y%m%d_%H%M%S')}"
        self._modified = False

    def mark_modified(self):
        """
        This makes codelist as modified
        Flag used to execute _update_version on save
        """
        self._modified = True

@dataclass
class Definition:
    """
    Collection of codelists that define the same broad clinical concept
    """
    definition_name: str
    definition_type: str = "OBSERVATION" # only this one for now - enum later
    codelists: Dict[str, Codelist] = field(default_factory=dict)
    definition_source: str = "CUSTOM"
    # created_datetime should be immutable
    created_datetime: str = field(default_factory=lambda: datetime.datetime.now().isoformat())
    updated_datetime: str = field(default=None)
    definition_id: str = field(default=None)
    definition_version: str = field(default=None)
    _modified: bool = field(default=True) # flag updates

    def __post_init__(self):
        """
        Init a definition ID and version *if not provided on a load*.
        Creates 8-character hash ID based on definition name and timestamp at init
        Initialises version string if not provided (see _update_version).
        """
        # set if not provided
        if self.updated_datetime is None:
            self.updated_datetime = self.created_datetime

        # create ID only if not provided (new definition)
        if self.definition_id is None:
            content = f"{self.definition_name}_{self.created_datetime}"
            self.definition_id = hashlib.md5(content.encode()).hexdigest()[:8]

        # create initial version if not provided
        if self.definition_version is None:
            self._update_version()

    def _update_version(self):
        """
        Update version and set modified flag to False
        """
        timestamp = datetime.datetime.now()
        self.definition_version = f"{self.definition_name}_{timestamp.strftime('%Y%m%d_%H%M%S')}"
        self._modified = False

    def mark_modified(self):
        """
        Mark this definition as modified
        """
        self._modified = True

    def add_code(self, code: Code) -> bool:
        """
        Add a code to the appropriate codelist
        Args:
            code(Code):
                Code object to add
        Returns:
            bool:
                True if code was added, False if duplicate
        """
        vocabulary = code.vocabulary

        # check if code already exists
        for existing_codelist in self.codelists.values():
            for existing_code in existing_codelist.codes:
                if existing_code.code == code.code and existing_code.vocabulary == code.vocabulary:
                    return False  # code already exists, don't add dupe

        # create a new codelist if vocabulary doesn't exist
        if vocabulary not in self.codelists:
            codelist_name = f"{self.definition_name}_{vocabulary}"
            self.codelists[vocabulary] = Codelist(codelist_name=codelist_name)

        # add code to the vocabulary's codelist
        self.codelists[vocabulary].codes.append(code)

        # mark as modified
        self.codelists[vocabulary].mark_modified()
        self._modified = True

        return True

    def remove_code(self, code_to_remove: Code) -> bool:
        """
        Remove a code from the definition
        Args:
            code_to_remove(Code):
                Code object to remove
        Returns:
            bool:
                True if code removed, False otherwise
        """
        vocabulary = code_to_remove.vocabulary
        if vocabulary in self.codelists:
            codelist = self.codelists[vocabulary]
            for i, code in enumerate(codelist.codes):
                if code.code == code_to_remove.code:
                    codelist.codes.pop(i)

                    # mark as modified
                    codelist.mark_modified()
                    self._modified = True

                    # remove codelist if last code is removed
                    if not codelist.codes:
                        del self.codelists[vocabulary]

                    return True
        return False

    def to_dict(self) -> dict:
        """
        Convert definition to a dictionary for json
        """
        result = {
            "definition_id": self.definition_id,
            "definition_name": self.definition_name,
            "definition_version": self.definition_version,
            "definition_source": self.definition_source,
            "created_datetime": self.created_datetime,
            "updated_datetime": self.updated_datetime,
            "definition_type": self.definition_type,
            "codelists": [],
        }

        for vocabulary, codelist in self.codelists.items():
            codelist_dict = {
                "codelist_id": codelist.codelist_id,
                "codelist_name": codelist.codelist_name,
                "codelist_version": codelist.codelist_version,
                "codes": [],
            }

            for code in codelist.codes:
                codelist_dict["codes"].append(
                    {
                        "code": code.code,
                        "code_description": code.code_description,
                        "vocabulary": code.vocabulary,
                    }
                )

            result["codelists"].append(codelist_dict)

        return result

    def save_to_json(self, directory: str = "data/definitions") -> str:
        """
        Save definition to json and update versions if modified

        Args:
            directory(str):
                dir where json saved (default "data/definitions")
        Return:
            str:
                Path where saved (show in streamlit)
        """
        # update definition version/date if modified
        if self._modified:
            self.updated_datetime = datetime.datetime.now().isoformat()
            self._update_version()

        # update codelist version if modified
        for codelist in self.codelists.values():
            if codelist._modified:
                codelist._update_version()

        # continue with save...
        os.makedirs(directory, exist_ok=True)

        filename = f"{self.definition_name}_{self.definition_id}.json"
        filepath = os.path.join(directory, filename)

        with open(filepath, "w") as f:
            json.dump(self.to_dict(), f, indent=2)

        return filepath

# could be @classmethod?
def definition_from_dict(data: dict) -> Definition:
    """
    Create a Definition object from dict
    IDs and versions are preserved
    """
    definition = Definition(
        definition_name=data["definition_name"],
        definition_type=data.get("definition_type", "OBSERVATION"),
        definition_source=data.get("definition_source", "CUSTOM"),
        created_datetime=data.get("created_datetime"),
        updated_datetime=data.get("updated_datetime"),
        definition_id=data.get("definition_id"),
        definition_version=data.get("definition_version"),
        _modified=False  # fresh load
    )

    # load codelists
    for codelist_data in data.get("codelists", []):
        codelist = Codelist(
            codelist_name=codelist_data["codelist_name"],
            codelist_id=codelist_data.get("codelist_id"),
            codelist_version=codelist_data.get("codelist_version"),
            _modified=False  # fresh load
        )

        # load codes
        for code_data in codelist_data.get("codes", []):
            code = Code(
                code=code_data["code"],
                code_description=code_data["code_description"],
                vocabulary=code_data["vocabulary"],
            )
            codelist.codes.append(code)

        # use the vocabulary of the first code as the dictionary key
        if codelist.codes:
            definition.codelists[codelist.codes[0].vocabulary] = codelist

    return definition

# could be @classmethod?
def load_definition_from_json(filepath: str) -> Definition:
    """
    Load Definition from json file
    """
    with open(filepath, "r") as f:
        data = json.load(f)

    return definition_from_dict(data)
