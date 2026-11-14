**Data-quality rule violations** — 18 rows (showing 18; full table: `results/prep/dq_issue_summary.csv`)

| table | rule | violations | example_key | example_detail |
| --- | --- | --- | --- | --- |
| name.basics | alive_over_122y | 2537 | nm1165446 | 1885 |
| name.basics | fk_knownFor_title_missing | 218 | nm15296524 | tt29297961 |
| name.basics | missing_name | 89 | nm10730114 |  |
| name.basics | death_before_birth | 28 | nm17520180 | 1899-1873 |
| title.akas | fk_title_missing | 1 | tt50921922 | tt50921922 |
| title.basics | missing_genres | 547893 | tt6777322 | tvEpisode\|2014\|44\|0 |
| title.basics | runtime_extreme_gt_24h | 210 | tt10059232 | tvSpecial\|2019\|1441\|0 |
| title.basics | runtime_non_positive | 4 | tt0830188 | short\|2005\|0\|0 |
| title.basics | start_in_far_future | 3 | tt5174640 | short\|2115\|0 |
| title.crew | fk_person_missing | 1472 | tt43441676 | nm18589809 |
| title.episode | missing_episode_number | 2120813 | tt11684510 | tt0409629 |
| title.episode | missing_season_number | 2120813 | tt4237662 | tt0407358 |
| title.episode | episode_before_series_start | 3559 | tt13828980 | tt13828724\|2020\|2019 |
| title.episode | season_number_gt_1000 | 1361 | tt38047774 | 2025 |
| title.episode | fk_parent_series_missing | 5 | tt28611150 | tt28606619 |
| title.principals | credited_gt_5y_after_death | 206296 | tt0072312 | nm0416721\|writer\|1936\|1974 |
| title.principals | fk_person_missing | 7976 | tt43441291 | nm18589809 |
| title.principals | credited_before_birth | 3110 | tt13133524 | nm10330409\|2017\|2012 |
