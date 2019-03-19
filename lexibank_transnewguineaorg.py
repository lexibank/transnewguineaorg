# coding=utf-8
from __future__ import unicode_literals, print_function
import re
from os import remove
from datetime import datetime

import lingpy
from pybtex.database import parse_string  # dependency of pycldf, so should be installed.
from pybtex.utils import OrderedCaseInsensitiveDict
from clldutils import jsonlib
from clldutils.path import Path
from pylexibank.util import get_url, jsondump
from pylexibank.dataset import Metadata
from pylexibank.dataset import Dataset as BaseDataset

LIMIT = 2000  # how many records to fetch at once

BASE_URL = "http://transnewguinea.org"

LANGUAGES_URL = BASE_URL + "/api/v1/language/?limit=%(limit)d"
RECORDS_URL = BASE_URL + "/api/v1/lexicon/?limit=%(limit)d&language=%(language)d"
WORDS_URL = BASE_URL + "/api/v1/word/?limit=%(limit)d"
SOURCES_URL = BASE_URL + "/api/v1/source/?limit=%(limit)d"


class Dataset(BaseDataset):
    id = "transnewguineaorg"
    dir = Path(__file__).parent

    def get_slug_from_uri(self, uri):
        return [_ for _ in uri.split("/") if _][-1]

    def cmd_install(self, **kw):
        languages = {o['slug']: o for o in jsonlib.load(Path(self.raw, "languages.json"))}
        words = {o['slug']: o for o in jsonlib.load(Path(self.raw, "words.json"))}
        sources = {o['slug']: o for o in jsonlib.load(Path(self.raw, "sources.json"))}
        
        with self.cldf as ds:
            # handle sources
            # want to make sure that the bibtex key matches our source id.
            self.log.info("Loading sources...")
            for s in sorted(sources):
                # this is ugly, I wish pybtex made this easier!
                bib = parse_string(sources[s]['bibtex'], 'bibtex')
                old_key = bib.entries.keys()[0]
                bib.entries[old_key].key = s
                bib.entries = OrderedCaseInsensitiveDict([(s, bib.entries[old_key])])
                ds.add_sources(bib)
            
            # handle languages
            self.log.info("Loading languages...")
            for l in sorted(languages):
                ds.add_language(
                    ID=l,
                    Name=languages[l]['fullname'],
                    ISO639P3code=languages[l]['isocode'],
                    Glottocode=languages[l]['glottocode']
                )
            
            # handle concepts
            self.log.info("Loading concepts...")
            for c in sorted(words):
                ds.add_concept(
                    ID=c,
                    Name=words[c]['word'],
                    Concepticon_ID=words[c]['concepticon_id'],
                    Concepticon_Gloss=words[c]['concepticon_gloss']
                )
            
            itemfiles = [f for f in self.raw.iterdir() if f.name.startswith("language-")]
            for filename in sorted(itemfiles):
                self.log.info("Loading data from %s..." % filename)
                for o in sorted(jsonlib.load(filename), key=lambda d: d['id']):
                    for form in self.split_forms(o, o['entry']):
                        ds.add_lexemes(
                            #ID=o['id'],
                            Local_ID=o['id'],
                            Language_ID=self.get_slug_from_uri(o['language']),
                            Parameter_ID=self.get_slug_from_uri(o['word']),
                            Value=o['entry'],
                            Source=self.get_slug_from_uri(o['source']),
                            Comment=o['annotation']
                        )

    def get_all(self, url):
        while True:
            j = get_url(url, self.log).json()
            yield j['objects']
            if not j['meta']['next']:
                break
            url = BASE_URL + j['meta']['next']
    
    def cmd_download(self, **kw):
        if not self.raw.exists():
            self.raw.mkdir()

        for fname in self.raw.iterdir():
            remove(fname)

        # sources
        sources = []
        for j in self.get_all(SOURCES_URL % {'limit': LIMIT}):
            sources.extend(j)
        jsondump(sources, Path(self.raw, "sources.json"), self.log)

        # languages
        languages = []
        for j in self.get_all(LANGUAGES_URL % {'limit': LIMIT}):
            languages.extend(j)
        jsondump(languages, Path(self.raw, "languages.json"), self.log)

        # words
        words = []
        for j in self.get_all(WORDS_URL % {'limit': LIMIT}):
            words.extend(j)
        jsondump(words, Path(self.raw, "words.json"), self.log)

        # items
        for language in languages:
            items = []
            for j in self.get_all(RECORDS_URL % {'limit': LIMIT, 'language': language['id']}):
                items.extend(j)
            jsondump(items, Path(self.raw, "language-%d.json" % language['id']), self.log)
        
        # version information
        with open(Path(self.raw, "version.txt"), 'w') as handle:
            handle.write(str(datetime.now()))
        