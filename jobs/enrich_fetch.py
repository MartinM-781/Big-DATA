"""Enrichment step 2/3 (plain Python): polite gathering from Wikimedia.

Source chain (single source family, human-authored, CC BY-SA):

1. **Wikidata Query Service** resolves IMDb ids (property P345) to the
   English Wikipedia article of the same entity. Matching on the IMDb id is
   exact, so no fuzzy title search is needed.
2. **English Wikipedia Action API**: for films, the wikitext of the latest
   revision (``prop=revisions``, 50 articles per call) so that the *Plot*
   section can be extracted and de-marked-up later in Spark; for people, the
   plain-text lead section (``prop=extracts&exintro``, 20 per call).

Politeness / policy compliance
------------------------------
* A descriptive User-Agent with a contact (Wikimedia User-Agent policy).
* Strictly sequential requests with a minimum interval between calls
  (default 1 s, at most 1 req/s; wikitext is batched by 10 pages),
  ``maxlag=5`` so we back off when the servers are lagging, and back-off
  honouring ``Retry-After`` on HTTP 429/5xx.
* Only API endpoints are used (no HTML scraping), as recommended by
  https://www.mediawiki.org/wiki/API:Etiquette ; robots.txt of the
  target hosts allows ``/w/api.php`` and ``/sparql``.
* Every record stores its source URL, revision id and retrieval time.

Output is raw JSON Lines (no cleaning here; cleaning is done in Spark).
The script is resumable: ids already present in the output are skipped.
"""

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote

import requests

from imdb_project import config

WDQS = "https://query.wikidata.org/sparql"
WIKI_API = "https://en.wikipedia.org/w/api.php"
CONTACT = os.environ.get("WIKI_CONTACT", "educational project, no contact")
USER_AGENT = (f"ECE-BigData-IMDb-Enrichment/1.0 ({CONTACT}) "
              f"python-requests/{requests.__version__}")


class PoliteSession:
    """requests.Session with rate limiting, retries and back-off."""

    def __init__(self, min_interval: float, max_retries: int = 6):
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": USER_AGENT,
                                     "Accept-Encoding": "gzip"})
        self.min_interval = min_interval
        self.max_retries = max_retries
        self._last = 0.0
        self.calls = 0

    def get(self, url: str, **kwargs) -> requests.Response:
        for attempt in range(self.max_retries):
            wait = self.min_interval - (time.monotonic() - self._last)
            if wait > 0:
                time.sleep(wait)
            self._last = time.monotonic()
            self.calls += 1
            try:
                resp = self.session.get(url, timeout=60, **kwargs)
            except requests.RequestException as exc:
                delay = 2 ** attempt
                print(f"  ! network error {exc!r}, retry in {delay}s")
                time.sleep(delay)
                continue
            lagged = (resp.ok and "maxlag" in resp.text[:300]
                      and '"error"' in resp.text[:50])
            if resp.status_code in (429, 500, 502, 503, 504) or lagged:
                delay = float(resp.headers.get("Retry-After", 2 ** attempt))
                print(f"  ! HTTP {resp.status_code}, back-off {delay:.0f}s")
                time.sleep(delay)
                continue
            resp.raise_for_status()
            return resp
        raise RuntimeError(f"giving up on {url}")


def now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def load_done(path: Path, key: str) -> set:
    done = set()
    if path.exists():
        with open(path, encoding="utf-8") as handle:
            for line in handle:
                try:
                    done.add(json.loads(line)[key])
                except (ValueError, KeyError):
                    continue
    return done


def chunks(values, size):
    for i in range(0, len(values), size):
        yield values[i:i + size]


# --------------------------------------------------------------------------
def resolve_wikidata(http: PoliteSession, ids: list, out: Path,
                     batch: int) -> None:
    """IMDb id -> (Wikidata item, enwiki title) via SPARQL, in batches."""
    done = load_done(out, "imdb_id")
    todo = [i for i in ids if i not in done]
    print(f"[wikidata] {len(todo):,} ids to resolve ({len(done):,} cached)")
    with open(out, "a", encoding="utf-8") as handle:
        for n, group in enumerate(chunks(todo, batch), 1):
            values = " ".join(f'"{i}"' for i in group)
            query = f"""
            SELECT ?imdb ?item ?article WHERE {{
              VALUES ?imdb {{ {values} }}
              ?item wdt:P345 ?imdb .
              OPTIONAL {{ ?article schema:about ?item ;
                         schema:isPartOf <https://en.wikipedia.org/> . }}
            }}"""
            resp = http.get(WDQS, params={"query": query, "format": "json"},
                            headers={"Accept": "application/sparql-results+json"})
            found = {}
            for b in resp.json()["results"]["bindings"]:
                imdb = b["imdb"]["value"]
                rec = found.setdefault(imdb, {"qid": None, "article_url": None})
                rec["qid"] = rec["qid"] or b["item"]["value"].rsplit("/", 1)[1]
                if "article" in b and not rec["article_url"]:
                    rec["article_url"] = b["article"]["value"]
            for imdb in group:
                rec = found.get(imdb, {"qid": None, "article_url": None})
                handle.write(json.dumps({
                    "imdb_id": imdb, **rec,
                    "lineage_query_url": WDQS,
                    "retrieved_at": now()}) + "\n")
            handle.flush()
            if n % 10 == 0:
                print(f"  {n * batch:,}/{len(todo):,}")


def resolve_languages(http: PoliteSession, ids: list, out: Path,
                      batch: int) -> None:
    """IMDb id -> original language(s) (Wikidata P364, ISO 639-1 P218)."""
    done = load_done(out, "imdb_id")
    todo = [i for i in ids if i not in done]
    print(f"[languages] {len(todo):,} films to resolve ({len(done):,} cached)")
    with open(out, "a", encoding="utf-8") as handle:
        for group in chunks(todo, batch):
            values = " ".join(f'"{i}"' for i in group)
            query = f"""
            SELECT ?imdb ?iso WHERE {{
              VALUES ?imdb {{ {values} }}
              ?item wdt:P345 ?imdb ; wdt:P364 ?lang .
              ?lang wdt:P218 ?iso .
            }}"""
            resp = http.get(WDQS, params={"query": query, "format": "json"},
                            headers={"Accept": "application/sparql-results+json"})
            langs = {}
            for b in resp.json()["results"]["bindings"]:
                langs.setdefault(b["imdb"]["value"], set()).add(
                    b["iso"]["value"])
            for imdb in group:
                handle.write(json.dumps({
                    "imdb_id": imdb, "languages": sorted(langs.get(imdb, [])),
                    "lineage_query_url": WDQS, "property": "P364",
                    "retrieved_at": now()}) + "\n")
            handle.flush()


def article_title(url: str) -> str:
    return requests.utils.unquote(url.rsplit("/wiki/", 1)[1]).replace("_", " ")


def fetch_extracts(http: PoliteSession, mapping: Path, out: Path,
                   intro_only: bool, batch: int, limit: int,
                   film_batch: int = 50) -> None:
    """Fetch plain-text extracts for every resolved article."""
    done = load_done(out, "imdb_id")
    todo = []
    with open(mapping, encoding="utf-8") as handle:
        for line in handle:
            rec = json.loads(line)
            if rec["imdb_id"] not in done:
                todo.append(rec)
    if limit:
        todo = todo[:limit]
    print(f"[wikipedia] {len(todo):,} records to fetch ({len(done):,} cached,"
          f" intro_only={intro_only})")
    with open(out, "a", encoding="utf-8") as handle:
        # Records without an article are written straight away so that the
        # Spark job can add an explicit "not available" note.
        with_article = []
        for rec in todo:
            if rec["article_url"]:
                with_article.append(rec)
            else:
                handle.write(json.dumps({
                    "imdb_id": rec["imdb_id"], "qid": rec["qid"],
                    "wiki_title": None, "source_url": None,
                    "revision_id": None, "revision_timestamp": None,
                    "extract": None, "status": "no_enwiki_article"
                    if rec["qid"] else "no_wikidata_item",
                    "retrieved_at": now()}) + "\n")
        # People: plain-text lead section (TextExtracts, 20 pages per call).
        # Films: the full article is needed for the Plot section; TextExtracts
        # only serves one full page per call, so we take the raw wikitext of
        # the latest revision instead (50 pages per call) and strip the
        # markup in Spark.
        size = batch if intro_only else film_batch
        for n, group in enumerate(chunks(with_article, size), 1):
            titles = {article_title(r["article_url"]): r for r in group}
            params = {
                "action": "query", "format": "json", "formatversion": "2",
                "prop": "revisions|info", "inprop": "url",
                "rvprop": "ids|timestamp", "redirects": "1", "maxlag": "5",
                "titles": "|".join(titles),
            }
            if intro_only:
                params.update({"prop": "extracts|revisions|info",
                               "explaintext": "1", "exintro": "1",
                               "exlimit": "max", "exsectionformat": "wiki"})
            else:
                params.update({"rvprop": "ids|timestamp|content",
                               "rvslots": "main"})
            data = http.get(WIKI_API, params=params).json()
            query = data.get("query", {})
            alias = {}
            for key in ("normalized", "redirects"):
                for item in query.get(key, []):
                    alias[item["to"]] = alias.get(item["from"], item["from"])
            for page in query.get("pages", []):
                original = alias.get(page["title"], page["title"])
                while original in alias and original not in titles:
                    original = alias[original]
                rec = titles.get(original) or titles.get(page["title"])
                if rec is None:
                    continue
                rev = (page.get("revisions") or [{}])[0]
                wikitext = (rev.get("slots", {}).get("main", {})
                            .get("content"))
                handle.write(json.dumps({
                    "wikitext": wikitext,
                    "imdb_id": rec["imdb_id"], "qid": rec["qid"],
                    "wiki_title": page["title"],
                    "source_url": page.get("fullurl") or
                    "https://en.wikipedia.org/wiki/" +
                    quote(page["title"].replace(" ", "_")),
                    "revision_id": rev.get("revid"),
                    "revision_timestamp": rev.get("timestamp"),
                    "extract": page.get("extract"),
                    "status": "missing_page" if page.get("missing")
                    else "ok",
                    "retrieved_at": now()}) + "\n")
                titles = {k: v for k, v in titles.items() if v is not rec}
            for rec in titles.values():      # not returned at all
                handle.write(json.dumps({
                    "imdb_id": rec["imdb_id"], "qid": rec["qid"],
                    "wiki_title": article_title(rec["article_url"]),
                    "source_url": rec["article_url"], "revision_id": None,
                    "revision_timestamp": None, "extract": None,
                    "status": "not_returned", "retrieved_at": now()}) + "\n")
            handle.flush()
            if n % 20 == 0:
                print(f"  {n * size:,}/{len(with_article):,} "
                      f"(http calls: {http.calls:,})")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.
                                     RawDescriptionHelpFormatter)
    parser.add_argument("--min-interval", type=float, default=1.0,
                        help="seconds between two HTTP calls")
    parser.add_argument("--sparql-batch", type=int, default=50)
    parser.add_argument("--film-batch", type=int, default=10,
                        help="wikitext pages per call (heavy responses)")
    parser.add_argument("--limit", type=int, default=0,
                        help="only fetch N records per entity (testing)")
    parser.add_argument("--only", choices=["languages", "films", "people"],
                        nargs="*", default=["languages", "films", "people"])
    args = parser.parse_args()

    http = PoliteSession(args.min_interval)
    raw_dir = config.ENRICH_DIR / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)
    if "languages" in args.only:
        ids = (config.ENRICH_DIR / "success_films.txt").read_text().split()
        resolve_languages(http, ids, raw_dir / "films_language.jsonl",
                          args.sparql_batch)
    for entity in [e for e in args.only if e != "languages"]:
        ids = (config.ENRICH_DIR / f"{entity}.txt").read_text().split()
        if args.limit:
            ids = ids[:args.limit]
        mapping = raw_dir / f"{entity}_wikidata.jsonl"
        resolve_wikidata(http, ids, mapping, args.sparql_batch)
        fetch_extracts(http, mapping, raw_dir / f"{entity}_wikipedia.jsonl",
                       intro_only=(entity == "people"), batch=20,
                       film_batch=args.film_batch,
                       limit=args.limit)
    print(f"[done] {http.calls:,} HTTP calls")
    return 0


if __name__ == "__main__":
    sys.exit(main())
