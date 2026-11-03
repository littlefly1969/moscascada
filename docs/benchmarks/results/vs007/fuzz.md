# VS-007 fuzz (libFuzzer, cargo-fuzz 0.13.2, nightly), 2026-11-03, host Linux 6.18.44-fc-v230 x86_64

Command: `cd fuzz && cargo +nightly fuzz run -O <target> <corpus> seeds/<target> -- -max_total_time=60 -rss_limit_mb=2048`

| target | result | last libFuzzer lines |
|---|---|---|
| modbus_decode | 0 crash, 0 OOM, 0 timeout | `#2872305	DONE   cov: 130 ft: 558 corp: 116/15652b lim: 4096 exec/s: 47086 rss: 624Mb Done 2872305 runs in 61 second(s) ` |
| value_decode | 0 crash, 0 OOM, 0 timeout | `#6280898	DONE   cov: 295 ft: 295 corp: 102/2911b lim: 4096 exec/s: 102965 rss: 494Mb Done 6280898 runs in 61 second(s) ` |
| catalog_parse | 0 crash, 0 OOM, 0 timeout | `#163758	DONE   cov: 3773 ft: 11643 corp: 1565/1999Kb lim: 2951 exec/s: 2684 rss: 425Mb Done 163758 runs in 61 second(s) ` |
