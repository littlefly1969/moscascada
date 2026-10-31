
#### `mixed hv=16 rare_interval_us=1000 queue=64`

| variant | n | batches_s median [min–max] (Δ vs pre_ct) | p50_ns µs median [min–max] (Δ vs pre_ct) | p99_ns µs median [min–max] (Δ vs pre_ct) | p999_ns µs median [min–max] (Δ vs pre_ct) | rare_p50_ns µs median [min–max] (Δ vs pre_ct) | rare_p99_ns µs median [min–max] (Δ vs pre_ct) | rare_p999_ns µs median [min–max] (Δ vs pre_ct) |
|---|---|---|---|---|---|---|---|---|
| pre_ct | 7 | – | – | – | – | 8349.1 [7142.4–10009.8] | 21294.8 [19217.2–29245.8] | 27648.0 [19951.2–51940.9] |
| final_ct | 7 | – | – | – | – | 7822.5 [7314.4–9257.4] (~-6.3%, p=0.342) | 20748.8 [14793.0–28120.6] (~-2.6%, p=0.340) | 25158.9 [18073.2–33783.8] (~-9.0%, p=0.576) |

#### `cell clients=1 size=1 pipeline=1`

| variant | n | batches_s median [min–max] (Δ vs pre_ct) | p50_ns µs median [min–max] (Δ vs pre_ct) | p99_ns µs median [min–max] (Δ vs pre_ct) | p999_ns µs median [min–max] (Δ vs pre_ct) | rare_p50_ns µs median [min–max] (Δ vs pre_ct) | rare_p99_ns µs median [min–max] (Δ vs pre_ct) | rare_p999_ns µs median [min–max] (Δ vs pre_ct) |
|---|---|---|---|---|---|---|---|---|
| pre_ct | 7 | 9125 [8616–9395] | 102.1 [95.35–108.2] | 218.0 [208.6–246.2] | 848.8 [602.2–1265.2] | – | – | – |
| final_ct | 7 | 8730 [8136–9923] (~-4.3%, p=0.712) | 106.8 [89.22–113.0] (~+4.6%, p=0.737) | 228.3 [200.0–264.7] (~+4.7%, p=0.643) | 728.7 [552.2–1021.2] (~-14.1%, p=0.417) | – | – | – |

#### `cell clients=1 size=128 pipeline=32`

| variant | n | batches_s median [min–max] (Δ vs pre_ct) | p50_ns µs median [min–max] (Δ vs pre_ct) | p99_ns µs median [min–max] (Δ vs pre_ct) | p999_ns µs median [min–max] (Δ vs pre_ct) | rare_p50_ns µs median [min–max] (Δ vs pre_ct) | rare_p99_ns µs median [min–max] (Δ vs pre_ct) | rare_p999_ns µs median [min–max] (Δ vs pre_ct) |
|---|---|---|---|---|---|---|---|---|
| pre_ct | 7 | 84729 [72219–94901] | 338.2 [319.0–366.4] | 968.1 [830.1–1206.1] | 2079.5 [1766.0–2631.2] | – | – | – |
| final_ct | 7 | 81074 [72601–90137] (~-4.3%, p=0.681) | 341.2 [322.0–410.3] (~+0.9%, p=0.374) | 1008.3 [886.7–1291.3] (~+4.1%, p=0.445) | 2065.7 [1706.7–2959.9] (~-0.7%, p=0.734) | – | – | – |

#### `cell clients=8 size=1 pipeline=32`

| variant | n | batches_s median [min–max] (Δ vs pre_ct) | p50_ns µs median [min–max] (Δ vs pre_ct) | p99_ns µs median [min–max] (Δ vs pre_ct) | p999_ns µs median [min–max] (Δ vs pre_ct) | rare_p50_ns µs median [min–max] (Δ vs pre_ct) | rare_p99_ns µs median [min–max] (Δ vs pre_ct) | rare_p999_ns µs median [min–max] (Δ vs pre_ct) |
|---|---|---|---|---|---|---|---|---|
| pre_ct | 7 | 244054 [237967–255391] | 860.4 [835.2–890.0] | 4296.0 [3992.1–4479.1] | 5753.0 [5155.2–7218.0] | – | – | – |
| final_ct | 7 | 227364 [217007–252370] (**-6.8%**, p=0.029) | 927.0 [826.6–980.8] (**+7.7%**, p=0.023) | 4324.7 [3981.3–4464.4] (~+0.7%, p=0.865) | 5419.8 [5120.6–6135.2] (~-5.8%, p=0.242) | – | – | – |

#### `cell clients=8 size=128 pipeline=32`

| variant | n | batches_s median [min–max] (Δ vs pre_ct) | p50_ns µs median [min–max] (Δ vs pre_ct) | p99_ns µs median [min–max] (Δ vs pre_ct) | p999_ns µs median [min–max] (Δ vs pre_ct) | rare_p50_ns µs median [min–max] (Δ vs pre_ct) | rare_p99_ns µs median [min–max] (Δ vs pre_ct) | rare_p999_ns µs median [min–max] (Δ vs pre_ct) |
|---|---|---|---|---|---|---|---|---|
| pre_ct | 7 | 77474 [62410–87573] | 2667.3 [2534.1–3853.5] | 9147.4 [7259.3–13550.2] | 12169.2 [9062.5–73633.7] | – | – | – |
| final_ct | 7 | 86878 [77130–96581] (~+12.1%, p=0.033) | 2579.3 [2278.1–2934.6] (~-3.3%, p=0.185) | 7913.8 [7161.9–9346.7] (~-13.5%, p=0.078) | 10886.5 [9873.1–16604.6] (~-10.5%, p=0.269) | – | – | – |
