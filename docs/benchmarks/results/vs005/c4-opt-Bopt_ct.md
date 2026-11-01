
#### `cell clients=8 size=128 pipeline=32`

| variant | n | batches_s median [min–max] (Δ vs Bopt_ct) | p50_ns µs median [min–max] (Δ vs Bopt_ct) | p99_ns µs median [min–max] (Δ vs Bopt_ct) | p999_ns µs median [min–max] (Δ vs Bopt_ct) | cpu_pct median [min–max] (Δ vs Bopt_ct) | rss_steady_kib median [min–max] (Δ vs Bopt_ct) |
|---|---|---|---|---|---|---|---|
| Bopt_ct | 10 | 105530 [89243–124315] | 2200.8 [1918.5–2418.6] | 5729.2 [4937.9–7768.3] | 8254.4 [6610.5–116111.3] | 143.8 [119.3–146.6] | 8414 [8368–8456] |
| Copt_ct | 10 | 106792 [104121–114389] (~+1.2%, p=0.556) | 2248.7 [2060.2–2338.5] (~+2.2%, p=0.625) | 5688.8 [5011.5–6111.2] (~-0.7%, p=0.223) | 8229.6 [6643.5–12623.7] (~-0.3%, p=0.611) | 145.9 [143.0–149.0] (~+1.5%, p=0.012) | 12160 [12064–12200] (**+44.5%**, p=0.000) |

#### `cell clients=16 size=128 pipeline=32`

| variant | n | batches_s median [min–max] (Δ vs Bopt_ct) | p50_ns µs median [min–max] (Δ vs Bopt_ct) | p99_ns µs median [min–max] (Δ vs Bopt_ct) | p999_ns µs median [min–max] (Δ vs Bopt_ct) | cpu_pct median [min–max] (Δ vs Bopt_ct) | rss_steady_kib median [min–max] (Δ vs Bopt_ct) |
|---|---|---|---|---|---|---|---|
| Bopt_ct | 10 | 96050 [84579–112470] | 5000.1 [4179.1–6073.6] | 11345.5 [9912.2–18956.7] | 15345.5 [11504.9–26680.4] | 139.3 [135.7–144.3] | 8462 [8404–8484] |
| Copt_ct | 10 | 100738 [91424–115261] (~+4.9%, p=0.214) | 4770.1 [4119.1–5151.2] (~-4.6%, p=0.203) | 11131.6 [10675.1–14916.8] (~-1.9%, p=0.763) | 15670.1 [13036.4–25732.2] (~+2.1%, p=0.561) | 142.2 [138.7–147.7] (~+2.0%, p=0.017) | 12174 [12048–12216] (**+43.9%**, p=0.000) |
