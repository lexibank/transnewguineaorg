import csv
from datetime import datetime
from os import remove
from pathlib import Path
from collections import defaultdict
from cldfbench.datadir import get_url
from pybtex.database import parse_string
from pybtex.utils import OrderedCaseInsensitiveDict
from pylexibank import FormSpec
from pylexibank.dataset import Dataset as BaseDataset
from pylexibank.util import progressbar, jsondump
from clldutils.misc import slug

LIMIT = 2000  # how many records to fetch at once

BASE_URL = "http://transnewguinea.org"

LANGUAGES_URL = BASE_URL + "/api/v1/language/?limit=%(limit)d"
RECORDS_URL = BASE_URL + "/api/v1/lexicon/?limit=%(limit)d&language=%(language)d"
WORDS_URL = BASE_URL + "/api/v1/word/?limit=%(limit)d"
SOURCES_URL = BASE_URL + "/api/v1/source/?limit=%(limit)d"


class Dataset(BaseDataset):
    id = "transnewguineaorg"
    dir = Path(__file__).parent
    writer_options = dict(keep_languages=False, keep_parameters=False)

    @staticmethod
    def get_slug_from_uri(uri):
        return [_ for _ in uri.split("/") if _][-1]

    form_spec = FormSpec(
        brackets={"(": ")", "[": "]"},
        separators=";/,|<",
        missing_data=("?", "-", "*", "---", "-BB:SRP", '*-', '*'),
        strip_inside_brackets=True,
        replacements=[
            (" ", "_"),
            ('_+_modif.', ''),
            ('_+_verb', ''),
            ('_+_PL', ''),
            ('_+_mdf', ''),
            ('_+_mod', ''),
            ("_+_'make", ''),
            ("ɬ ̥", "ɬ̥"),
            ("l ̥", "l̥"),
            ('"', "'"),
            (" ?", ""),
            ("91)", ""),
            ("') :", ""),
            ("a ͥ", "aj"),
            ("<<̋>>"[2:-2], ""),
            (" ̟", ""),
        ],
    )

    def cmd_makecldf(self, args):
        languages = {
            o["slug"]: o
            for o in self.raw_dir.read_json(self.raw_dir / "languages.json")
        }
        sources = {
            o["slug"]: o
            for o in self.raw_dir.read_json(self.raw_dir / "sources.json")
        }

        # handle sources
        # want to make sure that the bibtex key matches our source id.
        for source in sorted(sources):
            # this is ugly, I wish pybtex made this easier!
            print(source)
            bib = parse_string(sources[source]["bibtex"], "bibtex")
            old_key = list(bib.entries.keys())[0]
            bib.entries[old_key].key = source
            bib.entries = OrderedCaseInsensitiveDict([(source, bib.entries[old_key])])
            args.writer.add_sources(bib)

        # handle languages
        for lang in sorted(languages):
            args.writer.add_language(
                ID=lang,
                Name=languages[lang]["fullname"],
                ISO639P3code=languages[lang]["isocode"],
                Glottocode=languages[lang]["glottocode"])

        # handle concepts
        # TODO: can this be refactored?
        concepts = {}
        for concept in self.conceptlists[0].concepts.values():
            idx = '{0}_{1}'.format(concept.number, slug(concept.english))
            args.writer.add_concept(
                ID=idx,
                Name=concept.english,
                Concepticon_ID=concept.concepticon_id,
                Concepticon_Gloss=concept.concepticon_gloss)
            concepts[concept.english] = idx
            concepts[concept.english.replace(" ", "-")] = idx
            concepts[concept.english.replace(" ", "-").lower()] = idx
            concepts[slug(concept.english)] = idx
            concepts["-".join([slug(x) for x in concept.english.split()])] = idx

            if '(' in concept.english:
                new_string = concept.english[:concept.english.index('(')-1]
                concepts["-".join([slug(x) for x in new_string.split()])] = idx
                concepts[concept.english[:concept.english.index('(')-1]] = idx
                concepts[concept.english[:concept.english.index('(')-1].replace(' ', '-').lower()] = idx
            if concept.english.startswith("to "):
                new_string = concept.english[3:]
                concepts['-'.join([slug(x) for x in new_string.split()])] = idx
                concepts[concept.english.replace("to ", "")] = idx
        
        # these merge words into concepts listed in the concepticon wordlist.
        # note that this matches transnewguinea.org slugs
        concepts["mans-mother-law"] = concepts["man's mother in law"]
        concepts["brother-law"] = concepts["brother in law"]
        concepts["to-make-hole"] = concepts["make hole (in ground)"]
        concepts["front"] = concepts["in front"]
        concepts["husk-nut"] = concepts["husk (of nut)"]
        concepts["his"] = concepts["his, hers, its (pronoun p:3s)"]
        concepts["we-two-incl"] = concepts["we incl. dual (pronoun d:1p, incl, dual)"]
        concepts["intrnasitivizer"] = concepts["intransitivizer"]
        concepts["short-piece-wood"] = concepts["short-piece-of-wood"]
        concepts["top-foot"] = concepts["top (of foot)"]
        concepts["sit-feet-and-legs-together"] = concepts["sit (with feet and legs together)"]
        concepts["earth"] = concepts["earth/soil"]
        concepts["warm"] = concepts["warm/hot"]
        concepts["your-sg"] = concepts["your (pronoun: p:2s)"]
        concepts["-law"] = concepts["in-law"]
        concepts["to-roast"] = concepts["roast"]
        concepts["arrow-barred"] = concepts["arrow (barred) (Arrow with cross bar)"]
        concepts["them-dual"] = concepts["them (pronoun o:3p, dual)"]
        concepts["you-dual"] = concepts["you (pronoun d:2s)"]
        concepts["right-correct"] = concepts["right (correct, true)"]
        concepts["betelpepper"] = concepts["betelpepper vine"]
        concepts["to-chop"] = concepts["to chop, cut down"]
        concepts["road"] = concepts["road/path"]
        concepts["for-benefactive-clitic"] = concepts["for (benefactive) ((cliticised or suffixed to noun))"]
        concepts["mans-father-law"] = concepts["mans' father in law"]
        concepts["sister-law"] = concepts["sister in law"]
        concepts["you-o2s"] = concepts["you (pronoun o:2s)"]
        concepts["you-pl-o2p"] = concepts["you pl. (pronoun o:2p)"]
        concepts["we-pl-incl"] = concepts["we incl. (pronoun d:1p, incl)"]
        concepts["in"] = concepts["in, inside"]
        concepts["not_know"] = concepts["not know"]
        concepts["their-dual"] = concepts["their (pronoun p:3p, dual)"]
        concepts["blow-fire"] = concepts["blow (on fire)"]
        concepts["blunt-eg-knife"] = concepts["blunt (of e.g. knife)"]
        concepts["our-dual"] = concepts["our (two) (pronoun p:1p, dual)"]
        concepts["your-pl-dual"] = concepts["your (two) pl (pronoun p:2p, dual)"]
        concepts["suck-breast"] = concepts["to suck at breast"]
        concepts["draw-water-carry"] = concepts["draw water / carry"]
        concepts["tree-sp-Gnetum-gnemon"] = concepts["tree sp. (Gnetum gnemon)"]
        concepts["he-she"] = concepts["he, she, it, that, those"]
        concepts["fed"] = concepts["fed up (with)"]
        concepts["you-pl-dual-o2p"] = concepts["you plural two (pronoun d:2p, dual)"]
        concepts["you-pl-dual"] = concepts["you two (pronoun d:2s, dual)"]
        concepts["to-put"] = concepts["to put, give"]
        concepts["he-she-it-those"] = concepts["he, she, it, that, those"]
        concepts["we-two-excl"] = concepts["we excl. dual (pronoun d:1p, excl, dual)"]
        concepts["we-pl-excl"] = concepts["we excl. plural (pronoun d:1p, excl, plural)"]

        itemfiles = [
            f for f in self.raw_dir.iterdir() if f.name.startswith("language-")
        ]
        for filename in progressbar(sorted(itemfiles), desc="adding lexemes"):
            for o in sorted(
                self.raw_dir.read_json(filename), key=lambda d: d["id"]
            ):
                wordid = self.get_slug_from_uri(o['word'])
                lang_id = self.get_slug_from_uri(o["language"])

                if wordid in concepts:
                    args.writer.add_forms_from_value(
                        Local_ID=o["id"],
                        Language_ID=lang_id,
                        Parameter_ID=concepts[wordid],
                        Value=o["entry"],
                        Source=self.get_slug_from_uri(o["source"]),
                        Comment=o["annotation"],
                    )
                else:
                    args.log.info("error: concept not in concepticon wordlist: %s" % wordid)


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
                self.raw_dir / ("language-%d.json" % language["id"]),
                args.log,
            )

        # version information
        with open(self.raw_dir / "version.txt", "w") as handle:
            handle.write(str(datetime.now()))
