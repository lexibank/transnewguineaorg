from datetime import datetime
from os import remove
from pathlib import Path

from cldfbench.datadir import get_url
from pybtex.database import (
    parse_string,
)  # dependency of pycldf, so should be installed.
from pybtex.utils import OrderedCaseInsensitiveDict
from pylexibank import FormSpec
from pylexibank.dataset import Dataset as BaseDataset
from pylexibank.util import progressbar, jsondump

LIMIT = 2000  # how many records to fetch at once

BASE_URL = "http://transnewguinea.org"

LANGUAGES_URL = BASE_URL + "/api/v1/language/?limit=%(limit)d"
RECORDS_URL = (
    BASE_URL + "/api/v1/lexicon/?limit=%(limit)d&language=%(language)d"
)
WORDS_URL = BASE_URL + "/api/v1/word/?limit=%(limit)d"
SOURCES_URL = BASE_URL + "/api/v1/source/?limit=%(limit)d"


class Dataset(BaseDataset):
    id = "transnewguineaorg"
    dir = Path(__file__).parent

    @staticmethod
    def get_slug_from_uri(uri):
        return [_ for _ in uri.split("/") if _][-1]

    form_spec = FormSpec(
        brackets={"(": ")", "[": "]"},
        separators=";/,|<",
        missing_data=("?", "-", "*", "---"),
        strip_inside_brackets=True,
        replacements=[
            ("ɬ ̥", "ɬ̥"),
            ("l ̥", "l̥"),
            ('"', "'"),
            (" ?", ""),
            ("91)", ""),
            ("') :", ""),
            ("a ͥ", "aj"),
            ("̋y", "y"),
            (" ̟", ""),
        ],
    )

    def cmd_makecldf(self, args):
        languages = {
            o["slug"]: o
            for o in self.raw_dir.read_json(self.raw_dir / "languages.json")
        }
        words = {
            o["slug"]: o
            for o in self.raw_dir.read_json(self.raw_dir / "words.json")
        }
        sources = {
            o["slug"]: o
            for o in self.raw_dir.read_json(self.raw_dir / "sources.json")
        }
        # handle sources
        # want to make sure that the bibtex key matches our source id.
        for source in sorted(sources):
            # this is ugly, I wish pybtex made this easier!
            bib = parse_string(sources[source]["bibtex"], "bibtex")
            old_key = bib.entries.keys()[0]
            bib.entries[old_key].key = source
            bib.entries = OrderedCaseInsensitiveDict(
                [(source, bib.entries[old_key])]
            )
            args.writer.add_sources(bib)

        # handle languages
        for lang in progressbar(sorted(languages), desc="adding languages"):
            args.writer.add_language(
                ID=lang,
                Name=languages[lang]["fullname"],
                ISO639P3code=languages[lang]["isocode"],
                Glottocode=languages[lang]["glottocode"],
            )

        # handle concepts
        for concept in progressbar(sorted(words), desc="adding concepts"):
            args.writer.add_concept(
                ID=concept,
                # Local_ID=words[c]['id'],
                Name=words[concept]["word"],
                Concepticon_ID=words[concept]["concepticon_id"],
                Concepticon_Gloss=words[concept]["concepticon_gloss"],
            )

        itemfiles = [
            f for f in self.raw_dir.iterdir() if f.name.startswith("language-")
        ]
        for filename in progressbar(sorted(itemfiles), desc="adding lexemes"):
            for o in sorted(
                self.raw_dir.read_json(filename), key=lambda d: d["id"]
            ):
                args.writer.add_forms_from_value(
                    Local_ID=o["id"],
                    Language_ID=self.get_slug_from_uri(o["language"]),
                    Parameter_ID=self.get_slug_from_uri(o["word"]),
                    Value=o["entry"],
                    Source=self.get_slug_from_uri(o["source"]),
                    Comment=o["annotation"],
                )

    def get_all(self, url):
        """Helper function to iterate across the API's _next_ commands for a given URL"""
        while True:
            j = get_url(url).json()
            yield j["objects"]
            if not j["meta"]["next"]:
                break
            url = BASE_URL + j["meta"]["next"]

    def cmd_download(self, args):
        if not self.raw_dir.exists():
            self.raw_dir.mkdir()

        for fname in self.raw_dir.iterdir():
            remove(fname)

        # sources
        sources = []
        for j in self.get_all(SOURCES_URL % {"limit": LIMIT}):
            sources.extend(j)
        jsondump(sources, self.raw_dir / "sources.json", args.log)

        # languages
        languages = []
        for j in self.get_all(LANGUAGES_URL % {"limit": LIMIT}):
            languages.extend(j)
        jsondump(languages, self.raw_dir / "languages.json", args.log)

        # words
        words = []
        for j in self.get_all(WORDS_URL % {"limit": LIMIT}):
            words.extend(j)
        jsondump(words, self.raw_dir / "words.json", args.log)

        # items
        for language in languages:
            items = []
            for j in self.get_all(
                RECORDS_URL % {"limit": LIMIT, "language": language["id"]}
            ):
                items.extend(j)
            jsondump(
                items,
                self.raw_dir / "language-%d.json" % language["id"],
                args.log,
            )

        # version information
        with open(self.raw_dir / "version.txt", "w") as handle:
            handle.write(str(datetime.now()))
