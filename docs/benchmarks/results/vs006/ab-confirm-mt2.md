
#### `cell clients=1 size=1 pipeline=1`

| variant | n | batches_s median [min–max] (Δ vs A_mt2) | p50_ns µs median [min–max] (Δ vs A_mt2) | p99_ns µs median [min–max] (Δ vs A_mt2) | p999_ns µs median [min–max] (Δ vs A_mt2) | cpu_pct median [min–max] (Δ vs A_mt2) | rss_steady_kib median [min–max] (Δ vs A_mt2) |
|---|---|---|---|---|---|---|---|
| B_mt2 | 10 | 16556 [12588–16976] (~+4.0%, p=0.870) | 52.93 [51.94–65.93] (~-0.7%, p=0.850) | 142.2 [128.6–233.8] (~-7.8%, p=0.911) | 633.0 [471.6–1085.3] (~-4.3%, p=0.455) | 82.8 [81.0–84.3] (~+0.0%, p=1.000) | 9186 [9060–9328] (**+6.7%**, p=0.000) |

#### `cell clients=16 size=128 pipeline=32`

| variant | n | batches_s median [min–max] (Δ vs A_mt2) | p50_ns µs median [min–max] (Δ vs A_mt2) | p99_ns µs median [min–max] (Δ vs A_mt2) | p999_ns µs median [min–max] (Δ vs A_mt2) | cpu_pct median [min–max] (Δ vs A_mt2) | rss_steady_kib median [min–max] (Δ vs A_mt2) |
|---|---|---|---|---|---|---|---|
| B_mt2 | 10 | 196844 [168878–203646] (~+2.1%, p=0.767) | 2383.1 [2286.8–2684.2] (~-2.6%, p=0.921) | 6841.0 [6461.0–9062.8] (~+0.1%, p=0.631) | 9989.4 [8995.3–78454.1] (~-0.4%, p=0.719) | 203.4 [185.6–206.6] (~-0.3%, p=0.321) | 10152 [9972–10244] (**+6.3%**, p=0.000) |

#### `cell clients=8 size=128 pipeline=32`

| variant | n | batches_s median [min–max] (Δ vs A_mt2) | p50_ns µs median [min–max] (Δ vs A_mt2) | p99_ns µs median [min–max] (Δ vs A_mt2) | p999_ns µs median [min–max] (Δ vs A_mt2) | cpu_pct median [min–max] (Δ vs A_mt2) | rss_steady_kib median [min–max] (Δ vs A_mt2) |
|---|---|---|---|---|---|---|---|
| B_mt2 | 10 | 200738 [184126–213299] (~+3.2%, p=0.320) | 1161.1 [1103.1–1254.1] (~-2.5%, p=0.225) | 3448.6 [3088.1–4132.5] (~-2.0%, p=0.685) | 6101.6 [4368.9–8931.4] (~+11.2%, p=0.983) | 206.7 [201.6–215.0] (~+0.3%, p=0.456) | 9866 [9740–10000] (**+6.0%**, p=0.000) |
