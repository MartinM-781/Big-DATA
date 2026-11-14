# IMDb big data processing — report

All numbers below come from the CSV/Markdown files in [`results/`](results/),
produced by the jobs described in [PROJECT.md](PROJECT.md). The IMDb
snapshot was downloaded on 2026-11-12. Unless stated otherwise:

* a **movie** is a title with `titleType = movie`;
* a **feature-length film** is a movie with `runtimeMinutes >= 40`
  (AMPAS/AFI/BFI definition);
* a rating is only used when the title has **at least 100 votes**
  (`MIN_VOTES`), to avoid ratings that rest on a handful of votes;
* a **successful film** has `averageRating >= 7.0` and `numVotes >= 5,000`.

These parameters live in `src/imdb_project/config.py`.

## Table of contents

1. [Data preparation](#1-data-preparation)
2. [Data exploration](#2-data-exploration)
3. [Data enrichment](#3-data-enrichment)
4. [Streaming data processing](#4-streaming-data-processing)
5. [Open question](#5-open-question)

---

## 1. Data preparation

Script: `jobs/prepare.py`. Outputs: `results/prep/`, clean parquet layer in
`/data/clean`, quarantine/issue table in `/data/dq/issues`.

### 1.1 Volumes

| Table | Raw rows | Malformed rows | Clean rows |
| --- | ---: | ---: | ---: |
| name.basics | 15,808,370 | 0 | 15,808,370 |
| title.akas | 60,353,877 | 0 | 60,353,876 |
| title.basics | 12,953,566 | 0 | 12,953,566 |
| title.crew | 12,953,566 | 0 | 12,953,566 |
| title.episode | 10,026,289 | 0 | 10,026,284 |
| title.principals | 102,713,417 | 0 | 102,705,441 |
| title.ratings | 1,731,454 | 0 | 1,731,454 |

### 1.2 Percentage of null, missing or empty values per column

IMDb encodes missing values with the token `\N`. We counted, per column,
`\N`/null values and empty or blank strings (including empty lists such as
`[]`). **No empty strings were found**: IMDb uses `\N` consistently, so
`missing % = null %`.

| Table | Column | Null count | % missing |
| --- | --- | ---: | ---: |
| name.basics | nconst | 0 | 0.00 |
| name.basics | primaryName | 89 | 0.00 |
| name.basics | birthYear | 15,122,576 | 95.66 |
| name.basics | deathYear | 15,542,928 | 98.32 |
| name.basics | primaryProfession | 3,218,607 | 20.36 |
| name.basics | knownForTitles | 1,916,507 | 12.12 |
| title.akas | titleId | 0 | 0.00 |
| title.akas | ordering | 0 | 0.00 |
| title.akas | title | 0 | 0.00 |
| title.akas | region | 12,993,626 | 21.53 |
| title.akas | language | 19,649,615 | 32.56 |
| title.akas | types | 40,349,110 | 66.85 |
| title.akas | attributes | 60,036,655 | 99.47 |
| title.akas | isOriginalTitle | 0 | 0.00 |
| title.basics | tconst | 0 | 0.00 |
| title.basics | titleType | 0 | 0.00 |
| title.basics | primaryTitle | 0 | 0.00 |
| title.basics | originalTitle | 0 | 0.00 |
| title.basics | isAdult | 0 | 0.00 |
| title.basics | startYear | 1,487,454 | 11.48 |
| title.basics | endYear | 12,789,170 | 98.73 |
| title.basics | runtimeMinutes | 8,053,410 | 62.17 |
| title.basics | genres | 547,893 | 4.23 |
| title.crew | tconst | 0 | 0.00 |
| title.crew | directors | 5,785,700 | 44.66 |
| title.crew | writers | 6,426,788 | 49.61 |
| title.episode | tconst | 0 | 0.00 |
| title.episode | parentTconst | 0 | 0.00 |
| title.episode | seasonNumber | 2,120,813 | 21.15 |
| title.episode | episodeNumber | 2,120,813 | 21.15 |
| title.principals | tconst | 0 | 0.00 |
| title.principals | ordering | 0 | 0.00 |
| title.principals | nconst | 0 | 0.00 |
| title.principals | category | 0 | 0.00 |
| title.principals | job | 82,849,061 | 80.66 |
| title.principals | characters | 52,669,159 | 51.28 |
| title.ratings | tconst | 0 | 0.00 |
| title.ratings | averageRating | 0 | 0.00 |
| title.ratings | numVotes | 0 | 0.00 |

### 1.3 Top five columns by percentage of null values

| Rank | Column | % null |
| ---: | --- | ---: |
| 1 | `title.akas.attributes` | 99.47 |
| 2 | `title.basics.endYear` | 98.73 |
| 3 | `name.basics.deathYear` | 98.32 |
| 4 | `name.basics.birthYear` | 95.66 |
| 5 | `title.principals.job` | 80.66 |

Some of these are *structurally* null rather than defects: `endYear` only
makes sense for series, `deathYear` is null for living people, `job` is
only filled when the category is not self-explanatory. `birthYear` (95.7 %)
is the genuine data gap.

### 1.4 Other data-quality issues

Every rule below was evaluated on the full dataset; violations are stored
one row per offending key in `/data/dq/issues` (table, rule, key, detail).

| Table | Rule | Violations | Example |
| --- | --- | ---: | --- |
| name.basics | `death_before_birth` | 28 | nm17520180: born 1899, died 1873 |
| name.basics | `alive_over_122y` (born > 122 years ago, no death year) | 2,537 | nm1165446: born 1885 |
| name.basics | `missing_name` | 89 | nm10730114 |
| name.basics | `fk_knownFor_title_missing` | 218 | nm15296524 → tt29297961 |
| title.akas | `fk_title_missing` | 1 | tt50921922 |
| title.basics | `missing_genres` | 547,893 | tt6777322 |
| title.basics | `runtime_extreme_gt_24h` | 210 | tt10059232 (1,441 min) |
| title.basics | `runtime_non_positive` | 4 | tt0830188 (0 min) |
| title.basics | `start_in_far_future` (> 2031) | 3 | tt5174640 (2115) |
| title.crew | `fk_person_missing` | 1,472 | tt43441676 → nm18589809 |
| title.episode | `missing_season_number` / `missing_episode_number` | 2,120,813 | tt4237662 |
| title.episode | `episode_before_series_start` | 3,559 | episode 2019, series 2020 |
| title.episode | `season_number_gt_1000` (year used as season) | 1,361 | season 2025 |
| title.episode | `fk_parent_series_missing` | 5 | tt28611150 → tt28606619 |
| title.principals | `fk_person_missing` | 7,976 | tt43441291 → nm18589809 |
| title.principals | `credited_before_birth` | 3,110 | person born 2017 in a 2012 title |
| title.principals | `credited_gt_5y_after_death` (archive categories excluded) | 206,296 | writer died 1936, title 1974 |

Rules that were checked and found **no violation**: duplicated primary keys
(all tables), `end_before_start`, `end_year_on_non_series`,
`invalid_isAdult`, `invalid_titleType`, `more_than_3_genres`,
`rating_out_of_range`, `votes_non_positive`, title-level orphans in
ratings/crew/principals, `birth_in_future`, `death_in_future`,
`lifespan_over_122y`.

Two findings deserve a comment:

* **"Died before being born" is partly a format problem.** Among the 28
  cases are Aristotle (384–322), Sophocles (496–406) and Plautus (254–184):
  IMDb stores years *before Christ* as positive numbers, which the format
  cannot express. The others (e.g. 1899–1873) are plain typos.
* **Most posthumous credits are legitimate.** 206,296 credits come more than
  five years after the person's death, but most are writers of adapted works
  (Shakespeare, Dickens, ...). They are *flagged, not deleted*.

### 1.5 Handling strategy

| Issue | Handling | Reason |
| --- | --- | --- |
| `\N` tokens, blank strings | Converted to typed nulls; comma lists split into `array<string>` | Downstream code uses real types and nulls, never sentinel strings |
| Impossible values (death < birth, lifespan > 122 years, runtime ≤ 0, rating outside 1–10) | Offending field(s) set to null; rule name kept in a `dq_flags` array column; original values in the issue table | Keeps the rest of the row usable, makes the correction traceable and reversible |
| Suspicious but possible values (posthumous credits, > 24 h runtimes, very old people without death year) | Flagged only | They can be true (adaptations, art films); deleting them would bias results |
| Orphan foreign keys | Rows removed from the clean bridge tables (ratings, akas, episodes, principals); dangling ids removed from crew arrays; the issue table keeps them | Referential integrity in the clean layer, so inner joins neither silently lose nor invent rows |
| Duplicated keys | De-duplicated on the primary key (none found) | Idempotent re-runs |
| Missing `primaryTitle`/`originalTitle` | Filled from each other | Same entity, one of the two is enough |

### 1.6 Methodological explanation

**Methodology and why.** The raw TSVs are read **once** with an all-string
schema, a `PERMISSIVE` parser and a corrupt-record column. This lets us
profile the data exactly as delivered (`\N` vs empty string vs malformed
line) before any cast hides a problem. The profile is a single aggregation
per table (one pass over the data). The typed data is then written to a
parquet *staging* layer, because gzip is not splittable: every further read
of the TSV would be single-threaded. Validation is expressed as **named
rules**, each producing rows of a uniform issue table
`(table, rule, key, detail)`. This makes the checks auditable, easy to
extend, and directly reportable with `groupBy(table, rule)`. Foreign keys
are checked with `left_anti` joins on distinct key sets. Removing orphans
from the 100M-row `title.principals` table uses a broadcast anti-join
against the few thousand orphan pairs already found in the issue table, so
the big table is never shuffled again.

**Alternatives and trade-offs.**

* *Schema inference* would have been simpler but silently turns `\N` into
  strings or nulls depending on the column, which hides exactly what we
  must measure.
* *Dropping every row with a problem* is the simplest cleaning, but it would
  delete e.g. every person without a birth year (96 %) and bias every
  analysis. *Imputation* (median birth year, runtime by genre) was rejected
  because it invents facts and would distort Q1 and Q7.
* *Great Expectations / Deequ* offer declarative checks; we stayed with
  plain PySpark to avoid extra dependencies in the container, at the cost
  of writing the rule framework ourselves (about 30 lines).
* Orphan removal by semi-joining the 100M-row table with the 15M/13M key
  tables is the textbook approach. On an 8 GB machine it spilled more than
  20 GB to disk, so we replaced it with the broadcast anti-join described
  above (72 s instead of more than an hour).

---

## 2. Data exploration

Script: `jobs/explore.py` (one function per question). Outputs:
`results/explore/` (full CSV + Markdown preview for every table).

### 2.1 Feature films whose length differs notably from their genre's average

A film belongs to every genre listed in `genres` (exploded). For each genre we
compute the mean and standard deviation of the runtime of feature films, and
flag a film when `|runtime − genre mean| ≥ 3 × genre std` (|z| ≥ 3).
**7,609 (film, genre) outliers, 5,346 distinct films.**

| Genre | Films | Mean (min) | Std | Median | Long threshold (min) | Outliers | % |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Action | 40,386 | 101.0 | 29.7 | 94 | 190.0 | 345 | 0.85 |
| Adventure | 24,070 | 92.9 | 29.3 | 90 | 180.8 | 318 | 1.32 |
| Animation | 8,342 | 82.6 | 25.6 | 81 | 159.2 | 99 | 1.19 |
| Comedy | 96,681 | 93.0 | 21.5 | 91 | 157.4 | 1,049 | 1.09 |
| Crime | 34,652 | 95.5 | 24.5 | 92 | 169.1 | 317 | 0.91 |
| Documentary | 116,150 | 76.3 | 27.1 | 74 | 157.5 | 1,318 | 1.13 |
| Drama | 196,747 | 95.8 | 26.0 | 93 | 173.8 | 1,759 | 0.89 |
| Horror | 30,766 | 88.5 | 20.4 | 88 | 149.8 | 303 | 0.98 |
| Romance | 42,449 | 99.1 | 24.9 | 95 | 173.7 | 313 | 0.74 |
| Thriller | 34,111 | 96.1 | 21.6 | 93 | 160.8 | 302 | 0.89 |
| Western | 6,379 | 76.5 | 28.7 | 70 | 162.6 | 101 | 1.58 |

(26 genres in total, see `q1_runtime_outliers_by_genre.csv`.) Most extreme
films: *Logistics* (2012, Documentary, 51,420 min ≈ 36 days, z = 1,897),
*Ambiancé* (2020, 43,200 min), *Carnets Filmés (Liste Complète)* (2019,
28,643 min), *Modern Times Forever* (2011, 14,400 min); the first "classic"
fiction outliers are art films such as *The Cure for Insomnia* (1987,
5,220 min).

**Every outlier is "longer".** Because feature films start at 40 minutes and
`mean − 3σ` is below 40 minutes in every genre, no feature film can be
notably shorter than its genre average.

### 2.2 Top three movie genres per year (1980–2025) and 2.3 comparison with the other genres

Popularity of a genre in a year = total IMDb votes of the movies of that
genre released that year (audience engagement). The number of movies is the
tie-breaker. The per-year table compares the top 3 with all remaining genres:
vote share, share of (movie, genre) slots, vote-weighted rating, and the
ratio of votes per movie (top 3 / rest).

| Year | Top 3 genres | Top-3 vote share % | Top-3 movie share % | Top-3 rating | Rest rating | Votes/movie ratio |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| 1980 | Drama, Adventure, Horror | 44.3 | 36.3 | 7.73 | 7.50 | 1.39 |
| 1981 | Adventure, Drama, Horror | 46.5 | 36.1 | 7.24 | 6.85 | 1.54 |
| 1982 | Drama, Sci-Fi, Action | 43.7 | 39.5 | 7.56 | 7.15 | 1.19 |
| 1983 | Drama, Action, Adventure | 45.9 | 41.9 | 7.43 | 7.11 | 1.18 |
| 1984 | Action, Drama, Adventure | 44.1 | 40.0 | 7.35 | 7.18 | 1.18 |
| 1985 | Comedy, Adventure, Drama | 51.5 | 44.9 | 7.39 | 7.02 | 1.31 |
| 1986 | Drama, Adventure, Comedy | 46.2 | 45.9 | 7.20 | 7.05 | 1.01 |
| 1987 | Drama, Comedy, Action | 42.6 | 52.2 | 7.09 | 7.06 | 0.68 |
| 1988 | Drama, Comedy, Action | 46.8 | 50.1 | 7.16 | 7.07 | 0.88 |
| 1989 | Comedy, Drama, Adventure | 49.4 | 43.5 | 7.22 | 6.78 | 1.27 |
| 1990 | Drama, Comedy, Crime | 45.6 | 45.5 | 7.30 | 7.07 | 1.00 |
| 1991 | Drama, Crime, Comedy | 44.2 | 47.1 | 7.36 | 7.49 | 0.89 |
| 1992 | Drama, Comedy, Crime | 46.3 | 46.8 | 7.23 | 7.00 | 0.98 |
| 1993 | Drama, Comedy, Crime | 44.8 | 46.9 | 7.31 | 7.39 | 0.92 |
| 1994 | Drama, Crime, Comedy | 58.3 | 46.7 | 7.87 | 7.32 | 1.59 |
| 1995 | Drama, Crime, Comedy | 49.0 | 44.5 | 7.59 | 7.28 | 1.19 |
| 1996 | Drama, Comedy, Crime | 44.9 | 45.9 | 6.98 | 6.78 | 0.96 |
| 1997 | Drama, Action, Comedy | 44.1 | 49.4 | 7.09 | 7.13 | 0.81 |
| 1998 | Drama, Comedy, Crime | 51.5 | 46.2 | 7.40 | 6.80 | 1.23 |
| 1999 | Drama, Comedy, Crime | 43.6 | 46.3 | 7.53 | 7.32 | 0.90 |
| 2000 | Drama, Comedy, Adventure | 45.0 | 41.8 | 7.21 | 6.98 | 1.14 |
| 2001 | Drama, Comedy, Adventure | 42.4 | 42.1 | 7.36 | 7.16 | 1.01 |
| 2002 | Drama, Adventure, Action | 42.4 | 35.9 | 7.22 | 7.04 | 1.31 |
| 2003 | Drama, Action, Comedy | 45.5 | 46.5 | 7.11 | 7.22 | 0.96 |
| 2004 | Drama, Action, Comedy | 41.3 | 43.8 | 7.02 | 7.11 | 0.90 |
| 2005 | Drama, Comedy, Action | 40.4 | 42.9 | 6.79 | 6.87 | 0.90 |
| 2006 | Drama, Comedy, Thriller | 42.7 | 42.2 | 7.12 | 6.95 | 1.02 |
| 2007 | Drama, Comedy, Action | 38.3 | 41.5 | 6.96 | 6.91 | 0.88 |
| 2008 | Drama, Action, Comedy | 40.4 | 41.1 | 6.75 | 7.10 | 0.97 |
| 2009 | Drama, Comedy, Action | 41.3 | 40.8 | 6.98 | 6.88 | 1.02 |
| 2010 | Drama, Action, Adventure | 40.0 | 30.8 | 6.97 | 7.02 | 1.49 |
| 2011 | Drama, Action, Comedy | 46.1 | 38.5 | 6.84 | 6.75 | 1.37 |
| 2012 | Drama, Action, Comedy | 42.7 | 37.7 | 6.90 | 6.96 | 1.23 |
| 2013 | Drama, Action, Comedy | 41.5 | 37.5 | 6.84 | 6.95 | 1.18 |
| 2014 | Drama, Action, Adventure | 47.0 | 29.0 | 7.09 | 6.96 | 2.17 |
| 2015 | Drama, Action, Adventure | 46.6 | 30.1 | 6.98 | 6.77 | 2.03 |
| 2016 | Drama, Action, Adventure | 44.1 | 30.5 | 6.89 | 6.90 | 1.80 |
| 2017 | Drama, Action, Comedy | 45.0 | 39.3 | 6.93 | 6.80 | 1.26 |
| 2018 | Drama, Action, Adventure | 44.3 | 31.1 | 6.94 | 6.74 | 1.76 |
| 2019 | Drama, Action, Comedy | 43.6 | 39.6 | 7.00 | 6.85 | 1.18 |
| 2020 | Drama, Comedy, Action | 44.7 | 37.2 | 6.42 | 6.41 | 1.36 |
| 2021 | Drama, Action, Adventure | 46.4 | 31.9 | 6.78 | 6.54 | 1.85 |
| 2022 | Drama, Action, Comedy | 46.4 | 40.5 | 6.75 | 6.56 | 1.27 |
| 2023 | Drama, Action, Comedy | 44.9 | 40.9 | 6.67 | 6.66 | 1.18 |
| 2024 | Drama, Action, Comedy | 42.9 | 39.8 | 6.57 | 6.52 | 1.14 |
| 2025 | Drama, Action, Comedy | 39.3 | 38.6 | 6.51 | 6.46 | 1.03 |

How often each genre reaches the top 3 over the 46 years:

| Genre | Years in top 3 | Years ranked 1st | First / last year in top 3 |
| --- | ---: | ---: | --- |
| Drama | 46 | 42 | 1980 / 2025 |
| Comedy | 34 | 2 | 1985 / 2025 |
| Action | 29 | 1 | 1982 / 2025 |
| Adventure | 16 | 1 | 1980 / 2021 |
| Crime | 9 | 0 | 1990 / 1999 |
| Horror | 2 | 0 | 1980 / 1981 |
| Sci-Fi | 1 | 0 | 1982 / 1982 |
| Thriller | 1 | 0 | 2006 / 2006 |

**Comparison (Q3).**

* **Concentration.** Three genres out of 22–26 capture **38–58 %** of all
  votes every year (mean 44.8 %). The remaining ~21 genres share the rest.
* **Drama is structural**, Comedy and Action alternate. Crime belongs to the
  1990s top 3 (the *Pulp Fiction* / *Shawshank* era: 1994 is the most
  concentrated year, 58.3 %). Adventure rises again in the franchise years
  2014–2021.
* **Popularity is not only volume.** Before 2010 the top 3 have about as many
  votes per movie as the rest (ratio ≈ 1). From 2010 on, the ratio climbs to
  1.5–2.2 while the top-3 movie share *falls* to about 30 %. Fewer films get
  much more attention: the blockbuster effect.
* **Quality is similar.** The vote-weighted rating of the top 3 exceeds the
  rest by only 0.1–0.4 points in most years, and is even lower in 11 of the
  46 years (e.g. 1991, 2008, 2013). Popular genres are not "better" genres.

### 2.4 Directors with exactly one movie rated ≥ 8.0 and ≥ 2 other movies averaging ≤ 6.0

Movies with ≥ 100 votes. **181 directors.** Top by votes of their hit:

| Director | Hit (year) | Hit rating | Hit votes | Other movies | Avg rating of the others |
| --- | --- | ---: | ---: | ---: | ---: |
| James McTeigue | V for Vendetta (2005) | 8.1 | 1,261,677 | 4 | 5.97 |
| Richard Marquand | Star Wars: Episode VI – Return of the Jedi (1983) | 8.3 | 1,229,408 | 6 | 5.98 |
| M. Night Shyamalan | The Sixth Sense (1999) | 8.2 | 1,177,293 | 15 | 5.87 |
| Richard Kelly | Donnie Darko (2001) | 8.0 | 925,303 | 3 | 5.43 |
| Robert Rodriguez | Sin City (2005) | 8.0 | 825,699 | 23 | 5.90 |
| Frank Miller | Sin City (2005) | 8.0 | 825,699 | 2 | 5.65 |
| Martin Campbell | Casino Royale (2006) | 8.0 | 758,712 | 20 | 5.63 |
| John G. Avildsen | Rocky (1976) | 8.1 | 707,376 | 21 | 5.78 |
| Tate Taylor | The Help (2011) | 8.1 | 535,025 | 6 | 5.88 |
| Kirk Wise | Beauty and the Beast (1991) | 8.0 | 511,430 | 3 | 5.43 |

### 2.5 TV series with ≥ 3 seasons of ≥ 10 episodes, worst episode in the last season, best in the first two

Episodes with ≥ 10 votes; ties broken by number of votes. **475 series.**

| Series | Seasons (≥ 10 eps) | Rated episodes | Best | Rating | Worst | Rating |
| --- | ---: | ---: | --- | ---: | --- | ---: |
| House of Cards | 6 (5) | 73 | S2E1 | 9.5 | S6E8 | 2.5 |
| Narcos | 3 (3) | 30 | S2E10 | 9.4 | S3E1 | 7.8 |
| The Flash | 9 (9) | 184 | S1E15 | 9.6 | S9E6 | 3.8 |
| You | 5 (5) | 50 | S2E9 | 9.1 | S5E10 | 5.3 |
| 13 Reasons Why | 4 (4) | 49 | S1E13 | 9.2 | S4E4 | 5.1 |
| The Umbrella Academy | 4 (3) | 36 | S2E10 | 8.9 | S4E6 | 4.8 |
| Two and a Half Men | 12 (12) | 262 | S2E23 | 8.7 | S12E15 | 3.6 |
| True Blood | 7 (7) | 81 | S2E8 | 8.8 | S7E10 | 5.4 |
| Heroes | 4 (4) | 78 | S1E20 | 8.9 | S4E15 | 6.8 |
| One Punch Man | 3 (3) | 36 | S1E12 | 9.5 | S3E6 | 1.6 |
| 24 | 8 (8) | 192 | S1E24 | 9.3 | S8E9 | 7.6 |
| The Twilight Zone (1959) | 5 (5) | 156 | S2E6 | 9.1 | S5E27 | 5.6 |

### 2.6 Actors with successful films in several original languages

A film's original language comes from **Wikidata P364** (fetched by IMDb id,
human-curated). 6,748 of the 7,051 successful films (95.7 %) have it. For
229 more films we used an akas-based fallback (see methodology), giving
**98.95 % coverage**. A film with several Wikidata languages counts for one
of them only.

* **2,035 actors/actresses** have successful films in **≥ 2** languages.
* **324** have successful films in **≥ 3** languages.

| Actor | # languages | Languages | Successful films |
| --- | ---: | --- | ---: |
| Tony Leung Chiu-wai | 7 | de, en, es, fr, ja, vi, yue | 14 |
| Prakash Raj | 5 | en, hi, kn, ta, te | 41 |
| Mohanlal | 5 | en, hi, ml, ta, te | 24 |
| Dulquer Salmaan | 5 | en, hi, ml, ta, te | 15 |
| Murli Sharma | 5 | en, hi, ml, ta, te | 13 |
| Ashish Vidyarthi | 5 | en, hi, ml, ta, te | 12 |
| Atul Kulkarni | 5 | en, hi, kn, ta, te | 10 |
| Takeshi Kaneshiro | 5 | ca, de, en, fr, zh | 5 |
| Nassar | 4 | hi, ml, ta, te | 37 |
| Max von Sydow | 4 | da, en, fr, sv | 19 |

Successful films per language: en 4,133 · fr 429 · hi 366 · ja 298 ·
de 179 · es 172 · it 162 · ta 147 · tr 143 · ko 113 · ml 100 · te 82.
Multilingual careers are dominated by the **Indian film industries**
(Hindi, Tamil, Telugu, Malayalam, Kannada), where actors routinely work
across language markets.

### 2.7 Actors with 20+ year careers and 15+ movies who peaked early

Acting credits in movies with ≥ 100 votes. Career position of a film =
(year − first year) / (last year − first year). "Peaked early" = the
highest-rated movie lies in the **first third** of the career **and** the
mean rating of the first half exceeds that of the second half by
**≥ 0.5**. **10,712** careers qualify (≥ 20 years, ≥ 15 movies);
**2,327 (21.7 %)** peaked early.

| Actor | Career | Movies | Best movie (year, rating) | Early avg | Late avg | Decline |
| --- | --- | ---: | --- | ---: | ---: | ---: |
| Veronica Lake | 1941–1970 | 20 | Sullivan's Travels (1941, 7.9) | 6.42 | 2.50 | 3.92 |
| Aleksey Batalov | 1954–2007 | 16 | The Cranes Are Flying (1957, 8.3) | 7.35 | 4.05 | 3.30 |
| Norman Wisdom | 1953–2002 | 18 | A Stitch in Time (1963, 6.9) | 6.31 | 3.20 | 3.11 |
| Saroja Devi B. | 1958–2023 | 16 | Babruvahana (1977, 8.9) | 8.01 | 5.07 | 2.94 |
| Artur Rolén | 1918–1967 | 20 | The Outlaw and His Wife (1918, 7.2) | 7.20 | 3.85 | 3.35 |

The largest declines often come from a late, low-rated comeback film
(Veronica Lake's last film *Flesh Feast*, 1970).

### 2.8 Multi-talented professionals

Roles: **director** and **writer** from `title.crew`, **actor** from
`title.principals` (actor/actress). A person is multi-talented when they hold
≥ 2 distinct roles, each in ≥ 3 rated movies. For each person we compare the
mean rating of the movies where they held several roles with the movies
where they held one.

| Talents | People | Multi-role films | Single-role films | Mean rating multi | Mean rating single | Mean paired diff | % better multi |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| director + writer | 7,972 | 40,903 | 30,786 | 5.94 | 5.90 | +0.05 | 52.1 |
| actor + director + writer | 1,488 | 11,351 | 20,959 | 5.80 | 5.73 | +0.09 | 57.6 |
| actor + writer | 1,227 | 3,457 | 18,350 | 5.63 | 5.69 | +0.02 | 53.9 |
| actor + director | 454 | 908 | 9,886 | 5.69 | 5.84 | −0.11 | 48.6 |

Mean rating by the exact combination of roles held on one film:
director+writer 6.00 · actor 5.89 · director 5.95 · writer 5.93 ·
actor+director 5.75 · actor+writer 5.69 · actor+director+writer 5.66.

Examples: Woody Allen (54 films acting + directing + writing, 6.89 vs 5.69
otherwise, +1.20), Claude Chabrol (+0.46), Jean-Luc Godard (+0.29), while
Godfrey Ho does worse when combining roles (−0.44).

**Interpretation.** Writer-directors (*auteurs*) are the largest group and
do marginally better when they write their own film. Combining **acting and
directing** is the only combination with a negative effect: on average,
directing yourself costs about 0.1 point, and fewer than half of
actor-directors do better when they do both. The person-level paired
difference is the fairer measure. The pooled per-film means mix different
people and suffer from Simpson's paradox: actor+director+writer films have
the lowest pooled mean (5.66) but a positive paired difference.

### 2.9 Methodological explanation

**Methodology and why.**

* *All questions are DataFrame pipelines on the clean parquet layer*, with
  window functions for rankings (`row_number` per year, per series, per
  person) and conditional aggregations (`sum(when(...))`, `max(when(...,
  struct))`) instead of self-joins, so each question is one or two shuffles.
  The movies × ratings frame is cached because every question reuses it.
* *Q1*: the assignment speaks of the *average* length, so we used the genre
  mean with a 3-sigma rule, the textbook definition of "notably different".
  Mean and σ are computed over runtimes ≤ 600 min: a handful of 36-day art
  installations would otherwise inflate σ so much that nothing else could be
  an outlier. Every film is still tested.
* *Q2/Q3*: popularity = **sum of votes**, because a vote is an explicit sign
  of audience engagement. The number of releases measures production, not
  popularity.
* *Q4–Q8*: the ≥ 100 votes threshold removes ratings based on a handful of
  votes (family and friends), which otherwise create absurd 10/10
  "masterpieces". Q5 uses ≥ 10 votes per episode because episodes have far
  fewer votes than films.
* *Q6*: Wikidata P364 is exact and human-curated. Our first attempt, an
  akas-only heuristic, failed visibly: many regions (AE, NL, MX) reuse the
  romanised original title of foreign films, so *Parasite* collected Arabic
  and Dutch votes. That heuristic is kept only as a fallback (3 % of films).
* *Q7*: positions are normalised to [0, 1] so that careers of different
  lengths are comparable. Two conditions (early best film *and* early-half
  average higher) avoid flagging careers that merely had one early hit.
* *Q8*: a paired, per-person comparison (multi-role vs single-role films
  *of the same person*) to control for individual talent.

**Alternatives considered and trade-offs.**

* *Q1*: IQR (Tukey fences) or a robust z-score (median/MAD) handle skewed
  runtime distributions better, but they measure distance to the *median*,
  not to the average the question asks for. A per-genre percentile cut
  (top 1 %) would always flag exactly 1 % per genre, whatever the data.
* *Q2*: ranking by number of movies (production) or by mean rating
  (quality). Rating-based popularity would put niche genres such as
  Documentary or History on top. We kept vote counts and added the movie
  counts as a tie-breaker and as a column.
* *Q4*: "multiple other movies with an average ≤ 6.0" could also mean that
  each other movie is ≤ 6.0. We chose the average of the other movies (the
  most literal reading) and required at least 2 of them.
* *Q5*: we use the last season present in `title.episode`. A series still
  airing may get a worse episode later, so the answer is a snapshot.
* *Q6*: "success" could be defined relative to the language market (votes
  are dominated by English speakers). We kept one absolute threshold
  (≥ 7.0, ≥ 5,000 votes) for simplicity and explainability.
* *Q7*: a regression slope of rating vs year would be a smoother measure of
  decline, but it is sensitive to a single late outlier. The two-condition
  rule is easier to interpret.

All thresholds are parameters in `config.py`; changing them only re-runs
the job, no code change.

---

## 3. Data enrichment

Scripts: `jobs/enrich_select.py` (Spark), `jobs/enrich_fetch.py` (Python,
HTTP only), `jobs/enrich_build.py` (Spark). Outputs:
`/data/enrich/films_enriched` (parquet + JSON lines), `results/enrich/`.

### 3.1 Scope

For every year 1980–2025 and every genre, the **10 feature-length films with
the most votes** (ties broken by `tconst`), and for each film its **first
three principals** by IMDb `ordering`. A film listed under several genres is
selected once per genre but fetched once.

| metric | value |
| --- | --- |
| (genre, year) film slots | 10,542 |
| distinct films | 7,525 |
| film–principal links | 22,350 |
| distinct people | 11,609 |
| genres | 26 |

### 3.2 Results

| films | with synopsis | from the *Plot* section | from the lead section | avg. characters | coverage |
| --- | --- | --- | --- | --- | --- |
| 7,525 | 6,364 | 5,991 | 373 | 3,152 | **84.6 %** |

| people | with biography | avg. characters | coverage |
| --- | --- | --- | --- |
| 11,609 | 7,919 | 818 | **68.2 %** |

| principals with a biography, per film | films |
| --- | --- |
| 3 | 5,315 |
| 2 | 900 |
| 1 | 581 |
| 0 | 683 |
| no principal in IMDb (`-1`) | 46 |

Every missing value carries a **programmatic note** instead of a null
(`results/enrich/missing_notes.csv`):

| note | films | people |
| --- | --- | --- |
| Not available: no Wikidata item references this IMDb id (P345) | 829 | 2,481 |
| Not available: Wikidata item has no English Wikipedia article | 328 | 1,207 |
| Not available: empty extract | 4 | 2 |
| No *Plot* section in the article; lead section used (synopsis kept) | 373 | — |

Missing data is concentrated where expected: non-English productions
(Indian, Turkish, Japanese films with no English article) and secondary cast
members. `results/enrich/sample_2010.md` shows the Drama and Sci-Fi top 10 of
2010; all 20 synopses come from the *Plot* section. One full record
(*The Shawshank Redemption*) is in `results/enrich/sample_record.json`:

```json
"synopsis_lineage": {
  "source": "en.wikipedia.org",
  "source_url": "https://en.wikipedia.org/wiki/The_Shawshank_Redemption",
  "revision_id": 1384720571,
  "revision_timestamp": "2026-11-09T20:02:26.000Z",
  "wikidata_uri": "https://www.wikidata.org/wiki/Q172241",
  "retrieved_at": "2026-11-12T19:03:07.000Z",
  "license": "CC BY-SA 4.0 (Wikipedia contributors)"
}
```

### 3.3 Methodological explanation

**Source choice.** Wikipedia is free, human-written and licensed
CC BY-SA 4.0, which allows reuse with attribution. The task asks for lineage,
so it is stored per value. Commercial APIs (TMDb, OMDb) need keys and have
restrictive terms. We **do not generate any text with an LLM**: a generated
synopsis could not be traced to a source.

**Mapping IMDb → Wikipedia without guessing.** Matching on titles is
ambiguous (remakes, homonyms). We use Wikidata property **P345 (IMDb ID)**:
one SPARQL query per batch of 50 ids returns the Wikidata item and its
English Wikipedia sitelink. A title that is not in Wikidata is reported as
such rather than guessed. The same query gives the film's original language
(P364), reused by question 2.6.

**Collection (Python) vs. cleaning (Spark).** The fetcher only does HTTP and
appends raw JSON lines (`/data/enrich/raw/*.jsonl`). It is **resumable**: ids
already present are skipped, so an interrupted run restarts where it stopped.
It is **polite**: one sequential request at a time, at least 1–2 s apart,
`maxlag`, back-off on `429` / `Retry-After`, and a descriptive `User-Agent`.
The data are fetched as follows:

* Films use the Action API `prop=revisions` (wikitext of 10 articles per
  call), because plain-text extracts are limited to 1 page per call for full
  articles.
* People use `prop=extracts&exintro` (20 lead sections per call).

During the run Wikimedia answered **96** `429` responses out of 1,303
calls: requests without a contact address in the `User-Agent` get a lower
rate limit, so set `WIKI_CONTACT` in `.env`.

All cleaning is done in **Spark** (`enrich_build.py`):

1. The raw JSON lines are read with an **explicit schema**. Corrupt lines go
   to `_corrupt_record` and are counted. Only the latest record per id is
   kept (`max_by` on `retrieved_at`), so re-fetches are idempotent.
2. **Wikitext → plain text with Spark SQL regular expressions** (no Python
   UDF): remove comments, `<ref>` tags, templates `{{…}}` (nested ones in
   several passes), tables, files/images, and categories. Then
   `[[target|label]]` is turned into `label` and the bold/italic quotes and
   HTML entities are removed.
3. **Synopsis** = the *Plot* / *Plot summary* / *Synopsis* / *Story* /
   *Premise* section. If none exists, we use the lead section and record this
   in `synopsis_section` and in the note.
4. Join back to the selection (IMDb rating, votes, runtime), and nest the
   principals (with their biographies) as an array of structs. Each value has
   its own `*_lineage` struct: URL, revision id and timestamp (an article can
   be re-checked exactly as it was), Wikidata URI, retrieval time and licence.

**Scaling up.** The scope is a parameter (`--top-n`, `--principals`). The
Spark part scales linearly. The bottleneck is the API rate limit: about
10 films or 20 people per request at around 1 request/s. Enriching all
~300,000 feature films would take about 9 hours of polite crawling. For that
size the right tool is the monthly **Wikipedia/Wikidata dumps**
(`enwiki-pages-articles`, `wikidata-all.json`), read directly by Spark, with
the same regex cleaning and no API calls.

**Known limitations.**

* A person credited twice on the same film (for example actor and producer,
  like Natalie Portman in *Black Swan*) can take two of the three principal
  slots. De-duplicating `nconst` before taking the top 3 in
  `enrich_select.py` would fix it.
* Articles without a *Plot* section yield a shorter, more descriptive
  synopsis (373 films).
* Bios are lead sections, which mix biography and career summary.

---

## 4. Streaming data processing

Scripts: `producer/wiki_to_kafka.py`, `jobs/stream_lookup.py`,
`jobs/stream_process.py`, `jobs/stream_report.py`. Outputs:
`/data/stream/{matches,windows,volume}`, `results/stream/`.

### 4.1 Architecture

```text
Wikimedia EventStreams  --SSE-->  producer (Python)  -->  Kafka topic
 /v2/stream/recentchange            Last-Event-ID resume    wiki.recentchange
                                     late-event simulator           |
                                                                    v
             static IMDb lookup (parquet, broadcast)  -->  Spark Structured Streaming
             from stream_lookup.py                          parse -> filter -> match
                                                            -> dedup -> windows -> sinks
```

* **Producer.** It reads the server-sent events stream, skips canary events,
  and writes each event as raw JSON to Kafka, keyed by page title. On
  reconnect it resumes with `Last-Event-ID`. With `LATE_FRACTION=0.02` and
  `LATE_DELAY=900`, 2 % of the events are held back 15 minutes before being
  sent. This injects late data on purpose, to test the watermark.
* **Kafka** (3.8, KRaft, single broker) decouples ingestion from processing.
  If Spark stops, the events wait in the topic, and the job replays them from
  its checkpointed offsets.
* **Lookup (static).** It maps Wikipedia titles to IMDb entities with two
  kinds of keys:
  * `exact`: the article URL already resolved through Wikidata in Section 3
    (6,368 films).
  * `base`: the normalised IMDb name, without the parenthesised disambiguator.
    It covers 33,433 films and 10,240 series with at least 2,000 votes, and
    1,156,503 people known for a title with at least 20,000 votes.
* **Spark job.** It parses the events with an explicit schema (`from_json`)
  and keeps English Wikipedia (`enwiki`) article edits and page creations.
  It uses **event time** `meta.dt` and a **10-minute watermark**. It
  explodes each title into its two keys and joins them with the broadcast
  lookup (stream-static join). A rule then removes homonyms:
  * a disambiguator must agree with the entity type (`(2010 film)` must
    match the year, `(TV series)`, `(actor)`…);
  * a bare title is accepted only for very popular entities, and for people
    only if the name has at least two words.

  It removes re-deliveries with `dropDuplicatesWithinWatermark(event_uid,
  imdb_id)` and writes four queries:

  | query | output |
  | --- | --- |
  | `matches` | every match (parquet) |
  | `windows` | 5-minute tumbling windows of matched edits per entity type |
  | `volume` | 5-minute windows of all English Wikipedia edits, the match-rate denominator |
  | console | debug output |

  Each query has its own checkpoint. A `StreamingQueryListener` saves every
  progress report to `progress.jsonl`.

### 4.2 Results

Times are UTC. The job ran from about 18:02 to 19:48. It also replayed the
events left in the topic by a short test at 14:10 (`startingOffsets =
earliest`), which shows the Kafka replay.

| matched (event, entity) pairs | matched events | distinct IMDb entities |
| --- | --- | --- |
| 631 | 518 | 431 |

| entity type | confidence | events | entities | bot edits |
| --- | --- | --- | --- | --- |
| film | high (`exact`) | 69 | 57 | 26 |
| film | medium (`base`) | 69 | 39 | 20 |
| person | medium | 338 | 307 | 67 |
| series | medium | 43 | 28 | 11 |

Over 21 five-minute windows, **561 of 8,900 English Wikipedia article edits
(6.3 %) concern an IMDb entity**. The per-window rate ranges from 1.3 % to
10.4 % (`results/stream/match_rate.md`). The most edited entities were Ocean
Ramsey (29 edits), *All Good Things* (25), *Lupin* (7) and Gérard Depardieu
(6) (`results/stream/top_entities.md`). Bots account for about a quarter of
the matched edits.

**Structured Streaming metrics** (`results/stream/query_metrics.md`):

| query | micro-batches | input rows | avg. input rows/s | avg. processed rows/s | avg. batch ms | rows dropped by watermark | max. state rows |
| --- | --- | --- | --- | --- | --- | --- | --- |
| matches | 214 | 252,833 | 38.2 | 436 | 2,897 | **18** | 150 |
| windows | 214 | 252,833 | 38.2 | 388 | 3,246 | **18** | 158 |
| volume | 214 | 252,833 | 38.2 | 1,125 | 1,157 | **91** | 4 |

"Input rows" counts all Wikimedia events (every wiki), before the enwiki
filter. Processing capacity is about 10 times the arrival rate, so the job
keeps up with the stream. It emitted **53 windows** in append mode
(`results/stream/windows_emitted.md`).

**Lateness of the accepted matches** (Kafka ingest time minus event time):
p50 = 1 s, p99 = 2 s, max = 15 s, none above 10 minutes. The events held back
by the simulator (15 minutes, beyond the 10-minute watermark) do not appear
here. Spark **dropped** them, and they are the 18 matched rows and 91
volume rows counted in `numRowsDroppedByWatermark`.

### 4.3 Late data: what we did and why

* **Why it matters.** EventStreams is near real-time, but events can arrive
  late: the producer reconnects and replays from `Last-Event-ID`, Kafka may
  be stopped, or the Spark job may restart and replay. Aggregating on
  arrival time would put such an edit in the wrong window. We therefore
  aggregate on **event time** (`meta.dt`).
* **Watermark = 10 minutes.** The watermark is the maximum event time seen
  minus 10 minutes. Events older than it are dropped, which bounds the state
  kept for windows and de-duplication (at most about 150 rows here). The
  10 minutes is a trade-off: the observed lateness is at most 15 s, so
  10 minutes covers producer reconnections and short Kafka stops with a wide
  margin. A longer watermark would accept more late events but delay every
  result and keep more state.
* **Append mode.** Each 5-minute window is written **once**, when the
  watermark passes its end, so results are final and the file sink never
  needs updates. The cost is latency: a window appears about 15 minutes
  after its start.
* **Late data is measured, not silent.** The simulator delays 2 % of the
  events by 15 minutes. Spark counted the dropped rows (18 matched, 91 in
  volume). That proves the watermark works and gives the share of data lost
  to it.
* **Exactly-once output.** Kafka offsets and the state are checkpointed. File
  sinks commit each micro-batch atomically. `dropDuplicatesWithinWatermark`
  removes events delivered twice. A restart therefore neither loses nor
  double-counts matches.

**Alternatives.** If late events must not be lost:

* *update* mode with a mutable sink (Delta Lake / a database) re-emits a
  window when a late event arrives;
* or a larger watermark plus a nightly batch job that recomputes the windows
  from the `matches` table, which keeps every event.

**Operational note.** The run was planned for 15 minutes, but the first
version ended with `processAllAvailable()`. Because the producer keeps
writing, that call kept processing new data until we stopped it at
1 h 46 min. Memory pressure (3 GB driver, the 1.2 M-key lookup broadcast by
each of the four queries) made the batches slower near the end. The job now
just calls `stop()`. Nothing is lost, because every micro-batch is already
committed and checkpointed. The 1.2 M person keys are the memory cost of the
stream-static join. Restricting persons to a higher popularity, or using a
key-value store, would reduce it.

---

## 5. Open question

Script: `jobs/open_question.py`. Outputs: `results/open/`.

### 5.1 Question

> **Do recurring director–lead actor partnerships produce better films than
> one-off pairings, and does a partnership improve or wear off with each new
> collaboration?**

Film history is full of famous duos (Kurosawa–Mifune, Scorsese–De Niro,
Burton–Depp). Two opposite stories are often told: "trust and shared
language make each film better", and "the magic fades, the partnership
becomes a routine". Studios and casting directors make real decisions on
this belief. The question also has a statistical trap worth showing: good
directors make good films *and* attract repeat collaborators, so a naive
comparison confuses talent with partnership.

**Data.** Only IMDb tables are needed: `title.basics` (feature films, year),
`title.crew` (directors, an array exploded to one row per director),
`title.principals` (lead cast = the first three actor/actress credits by
billing order), `title.ratings` (≥ 100 votes) and `name.basics` (names).
That is a five-table join on 100M+ principal rows. No external data was
required.

### 5.2 Results

Scope: 214,897 (director, lead actor, film) rows for directors with at least
5 rated feature films. `rel_rating` = film rating − the director's mean
rating over all their films (director fixed effect).

**One-off vs recurring pairings**

| Pair type | Pairs | Pair-films | Mean rating | Mean rating vs director's mean |
| --- | ---: | ---: | ---: | ---: |
| One-off | 158,258 | 158,258 | 5.93 | −0.04 |
| Recurring (≥ 2 films) | 21,226 | 56,639 | 6.10 | +0.08 |

**Rating vs the director's mean, by collaboration number (recurring pairs)**

| Collaboration # | Films | Mean rating | vs director's mean | 95 % CI |
| --- | ---: | ---: | ---: | ---: |
| 1st | 21,226 | 6.21 | **+0.19** | ±0.01 |
| 2nd | 21,226 | 6.02 | +0.01 | ±0.01 |
| 3rd | 6,616 | 6.08 | +0.01 | ±0.02 |
| 4th | 2,873 | 6.07 | +0.00 | ±0.03 |
| 5th and more | 4,698 | 6.06 | +0.01 | ±0.02 |

**Length of the partnership**

| Films together | Pairs | vs director's mean | Mean rating |
| --- | ---: | ---: | ---: |
| 1 | 158,258 | −0.04 | 5.93 |
| 2 | 14,610 | +0.05 | 6.04 |
| 3 | 3,743 | +0.09 | 6.15 |
| 4 | 1,333 | +0.11 | 6.17 |
| 5 | 645 | +0.10 | 6.15 |
| 6+ | 895 | +0.12 | 6.18 |

**Within-pair trajectory (pairs with ≥ 3 films):** 6,616 pairs; the last
collaboration is rated on average **0.36 points lower** than the first
(relative to the director's mean), and **62.3 %** of these partnerships end
on a lower note than they started.

**By decade** (share of pair-films that belong to a recurring pair, and the
relative rating of recurring vs one-off pairings):

| Decade | Share recurring | Recurring vs director's mean | One-off vs director's mean |
| --- | ---: | ---: | ---: |
| 1930s | 0.35 | +0.04 | −0.06 |
| 1950s | 0.31 | +0.10 | +0.01 |
| 1970s | 0.31 | +0.09 | +0.01 |
| 1990s | 0.24 | +0.21 | +0.02 |
| 2000s | 0.21 | +0.10 | −0.05 |
| 2010s | 0.23 | −0.01 | −0.09 |
| 2020s | 0.22 | −0.09 | −0.16 |

Extremes among partnerships of ≥ 5 films (full lists:
`results/open/top_partnerships.csv`, `worst_partnerships.csv`):
Masud Kimiai–Behrouz Vossoughi (+1.43), Roger Corman–Vincent Price (+1.38)
and Dariush Mehrjui–Ezzatolah Entezami (+1.34) at the top;
Jack Hill–Boris Karloff (−1.42) and Blake Edwards–Herbert Lom in the late
*Pink Panther* sequels (last film −2.35) at the bottom.

### 5.3 Key takeaways

1. **Recurring pairs are better on average**: +0.08 vs −0.04 relative to
   the same director's mean. The premium grows with the length of the
   partnership (+0.12 for 6+ films).
2. **But the premium sits almost entirely in the first film.** The 1st
   collaboration of a pair that will work together again is +0.19 above the
   director's mean; from the 2nd film on, the pair is exactly average
   (+0.00 to +0.01). This is a **selection effect**, not a "chemistry"
   effect: directors re-hire an actor *because* their first film together
   worked, and later films regress to the director's mean.
3. **Partnerships tend to wear off**: in 62 % of pairs with three or more
   films the last film is worse than the first (−0.36 on average). The
   sequel-heavy pairs at the bottom of the ranking (Pink Panther, late
   Karloff horror) illustrate it.
4. Recurring partnerships were more common in the studio-system era (35 % of
   pair-films in the 1930s vs about 22 % today), and their relative
   advantage has disappeared since the 2010s.

### 5.4 Methodological explanation

**Methodology and why.** The analysis is a multi-table join (films ×
directors × lead actors) followed by window functions: the collaboration
index is `row_number()` over `(director, actor)` ordered by year, and pair
sizes are counts over the same window. The key design decision is the
**director fixed effect**: every film is compared with the mean of *its own
director*. Without it, recurring pairs would look better simply because
prolific, good directors (who have many repeat collaborators) dominate the
recurring group. The director's mean is computed over distinct films, so a
film is not weighted by its number of leads. "Lead" is the first three
acting credits, a standard proxy for top billing.

**Alternatives considered.**

* *Raw mean comparison* (one-off vs recurring): simpler, but confounded by
  director quality, as explained above. It overstates the effect.
* *A mixed-effects regression* (rating ~ collaboration number + random
  director and actor effects) would be the statistically richest model. It
  is not available in Spark MLlib and would require collecting the data to
  a single node. The fixed-effect difference captures the main confound
  with pure Spark aggregations and scales to any size.
* *Actor fixed effect* in addition to the director's: considered but not
  implemented, since actors in the selection have far fewer films than
  directors, which would make the actor means very noisy.
* *Weighting by votes* would favour blockbusters; we chose an unweighted
  mean per film because the question is about each collaboration, not about
  audience size.

**Limitations.** IMDb ratings measure audience appreciation, not artistic
quality. The selection effect also implies survivorship bias: pairs whose
first film failed rarely get a second chance, so we never observe them.
