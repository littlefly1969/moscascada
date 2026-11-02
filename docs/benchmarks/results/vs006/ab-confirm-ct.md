
#### `cell clients=1 size=1 pipeline=1`

| variant | n | batches_s median [min–max] (Δ vs A_ct) | p50_ns µs median [min–max] (Δ vs A_ct) | p99_ns µs median [min–max] (Δ vs A_ct) | p999_ns µs median [min–max] (Δ vs A_ct) | cpu_pct median [min–max] (Δ vs A_ct) | rss_steady_kib median [min–max] (Δ vs A_ct) |
|---|---|---|---|---|---|---|---|
| B_ct | 10 | 14439 [14023–15270] (~-0.6%, p=0.867) | 62.24 [59.83–64.42] (~+0.1%, p=0.855) | 145.9 [119.6–173.1] (~+2.2%, p=0.897) | 512.5 [321.7–807.8] (~-6.8%, p=0.990) | 71.3 [69.3–72.3] (~+1.1%, p=0.051) | 8898 [8812–8960] (**+6.5%**, p=0.000) |

#### `cell clients=16 size=128 pipeline=32`

| variant | n | batches_s median [min–max] (Δ vs A_ct) | p50_ns µs median [min–max] (Δ vs A_ct) | p99_ns µs median [min–max] (Δ vs A_ct) | p999_ns µs median [min–max] (Δ vs A_ct) | cpu_pct median [min–max] (Δ vs A_ct) | rss_steady_kib median [min–max] (Δ vs A_ct) |
|---|---|---|---|---|---|---|---|
| B_ct | 10 | 106972 [100199–110907] (~-3.7%, p=0.028) | 4442.1 [4109.4–4778.4] (~+5.0%, p=0.049) | 11171.2 [10187.4–13324.2] (~+4.6%, p=0.224) | 15077.5 [12758.6–35189.0] (~+2.8%, p=0.275) | 137.3 [134.0–140.7] (~-1.9%, p=0.035) | 8930 [8852–8988] (**+6.3%**, p=0.000) |

#### `cell clients=8 size=128 pipeline=32`

| variant | n | batches_s median [min–max] (Δ vs A_ct) | p50_ns µs median [min–max] (Δ vs A_ct) | p99_ns µs median [min–max] (Δ vs A_ct) | p999_ns µs median [min–max] (Δ vs A_ct) | cpu_pct median [min–max] (Δ vs A_ct) | rss_steady_kib median [min–max] (Δ vs A_ct) |
|---|---|---|---|---|---|---|---|
| B_ct | 10 | 113628 [106076–118008] (~-0.6%, p=0.072) | 2034.6 [1982.9–2147.9] (~+2.9%, p=0.007) | 5866.2 [5593.5–6681.5] (~-1.6%, p=0.765) | 9339.5 [7703.0–11425.6] (~+9.4%, p=0.622) | 140.2 [137.3–143.3] (~-1.6%, p=0.013) | 8934 [8864–8992] (**+6.6%**, p=0.000) |
