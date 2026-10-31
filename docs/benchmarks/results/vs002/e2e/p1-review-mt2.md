
#### `cell clients=8 size=128 pipeline=32`

| variant | n | batches_s median [min–max] (Δ vs pre) | p50_ns µs median [min–max] (Δ vs pre) | p99_ns µs median [min–max] (Δ vs pre) | p999_ns µs median [min–max] (Δ vs pre) | cpu_pct median [min–max] (Δ vs pre) | rss_steady_kib median [min–max] (Δ vs pre) |
|---|---|---|---|---|---|---|---|
| pre | 7 | 148570 [126130–157390] | 1567.3 [1464.1–1803.4] | 4872.3 [4354.4–5599.8] | 7936.2 [6732.3–34493.0] | 203.6 [199.3–208.6] | 9012 [8944–9100] |
| post | 7 | 136896 [100147–172353] (~-7.9%, p=0.611) | 1669.8 [1377.3–2317.7] (~+6.5%, p=0.439) | 4866.7 [3561.4–6868.4] (~-0.1%, p=0.831) | 8111.5 [4864.5–10048.1] (~+2.2%, p=0.444) | 210.8 [163.6–216.0] (~+3.5%, p=0.983) | 8940 [8896–9108] (~-0.8%, p=0.121) |

#### `cell clients=8 size=1 pipeline=32`

| variant | n | batches_s median [min–max] (Δ vs pre) | p50_ns µs median [min–max] (Δ vs pre) | p99_ns µs median [min–max] (Δ vs pre) | p999_ns µs median [min–max] (Δ vs pre) | cpu_pct median [min–max] (Δ vs pre) | rss_steady_kib median [min–max] (Δ vs pre) |
|---|---|---|---|---|---|---|---|
| pre | 7 | 479795 [409705–551794] | 484.4 [422.1–566.3] | 1481.4 [1248.2–1732.8] | 3489.7 [2667.5–3852.4] | 185.0 [176.0–194.0] | 8532 [8464–8580] |
| post | 7 | 470403 [422805–499792] (~-2.0%, p=0.448) | 492.5 [464.1–551.3] (~+1.7%, p=0.533) | 1530.9 [1377.6–1805.6] (~+3.3%, p=0.384) | 3624.5 [3234.8–4463.4] (~+3.9%, p=0.095) | 187.0 [181.0–189.3] (~+1.1%, p=0.689) | 8544 [8464–8632] (~+0.1%, p=0.581) |

#### `mixed hv=16 rare_interval_us=1000 queue=64`

| variant | n | batches_s median [min–max] (Δ vs pre) | p50_ns µs median [min–max] (Δ vs pre) | p99_ns µs median [min–max] (Δ vs pre) | p999_ns µs median [min–max] (Δ vs pre) | cpu_pct median [min–max] (Δ vs pre) | rss_steady_kib median [min–max] (Δ vs pre) |
|---|---|---|---|---|---|---|---|
| pre | 7 | – | – | – | – | 205.3 [196.3–210.3] | 9280 [9244–9304] |
| post | 7 | – | – | – | – | 207.0 [159.3–209.6] (~+0.8%, p=0.674) | 9308 [9204–9384] (~+0.3%, p=0.593) |

#### `cell clients=1 size=128 pipeline=32`

| variant | n | batches_s median [min–max] (Δ vs pre) | p50_ns µs median [min–max] (Δ vs pre) | p99_ns µs median [min–max] (Δ vs pre) | p999_ns µs median [min–max] (Δ vs pre) | cpu_pct median [min–max] (Δ vs pre) | rss_steady_kib median [min–max] (Δ vs pre) |
|---|---|---|---|---|---|---|---|
| pre | 7 | 57914 [47252–68645] | 514.9 [389.5–648.5] | 1431.0 [1313.6–1512.7] | 3012.8 [2437.8–4106.0] | 140.0 [136.3–145.0] | 8504 [8452–8552] |
| post | 7 | 47425 [41817–56935] (~-18.1%, p=0.044) | 612.5 [470.9–720.0] (~+19.0%, p=0.110) | 1495.8 [1384.5–1622.3] (~+4.5%, p=0.105) | 2814.6 [2412.5–6474.5] (~-6.6%, p=0.695) | 136.3 [134.0–140.6] (~-2.6%, p=0.036) | 8532 [8524–8552] (~+0.3%, p=0.027) |
