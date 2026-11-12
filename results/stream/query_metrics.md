**Structured Streaming metrics per query** — 4 rows (showing 4; full table: `results/stream/query_metrics.csv`)

| query | micro_batches | input_rows | avg_input_rps | avg_processed_rps | avg_trigger_ms | max_trigger_ms | dropped_by_watermark | max_state_rows | final_watermark |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| console | 214 | 252833 | 38.21 | 404.51 | 3,119.95 | 13074 | 18 | 150 | 2026-11-12T19:37:56.102Z |
| matches | 214 | 252833 | 38.20 | 435.99 | 2,896.90 | 12209 | 18 | 150 | 2026-11-12T19:37:56.102Z |
| volume | 214 | 252833 | 38.20 | 1,124.99 | 1,157.42 | 8065 | 91 | 4 | 2026-11-12T19:37:56.102Z |
| windows | 214 | 252833 | 38.20 | 387.76 | 3,245.75 | 11690 | 18 | 158 | 2026-11-12T19:37:56.102Z |
