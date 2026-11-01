
#### `cell clients=16 size=128 pipeline=32`

| variant | n | batches_s median [min–max] (Δ vs A_sup_ct) | p50_ns µs median [min–max] (Δ vs A_sup_ct) | p99_ns µs median [min–max] (Δ vs A_sup_ct) | p999_ns µs median [min–max] (Δ vs A_sup_ct) | cpu_pct median [min–max] (Δ vs A_sup_ct) | rss_steady_kib median [min–max] (Δ vs A_sup_ct) |
|---|---|---|---|---|---|---|---|
| A_sup_ct | 5 | 119596 [113048–122317] | 4029.2 [3764.7–4082.6] | 9674.9 [9066.6–9932.3] | 13762.6 [11987.5–16009.3] | 145.0 [144.0–146.0] | 8220 [8136–8256] |
| B_sup_ct | 5 | 115533 [110083–125168] (~-3.4%, p=0.563) | 4120.2 [3758.3–4426.2] (~+2.3%, p=0.437) | 9708.8 [8617.6–10639.1] (~+0.4%, p=0.754) | 13526.9 [11241.6–36683.4] (~-1.7%, p=0.849) | 142.7 [141.6–148.0] (~-1.6%, p=0.810) | 8444 [8416–8488] (**+2.7%**, p=0.008) |

#### `cell clients=1 size=1 pipeline=1`

| variant | n | batches_s median [min–max] (Δ vs A_sup_ct) | p50_ns µs median [min–max] (Δ vs A_sup_ct) | p99_ns µs median [min–max] (Δ vs A_sup_ct) | p999_ns µs median [min–max] (Δ vs A_sup_ct) | cpu_pct median [min–max] (Δ vs A_sup_ct) | rss_steady_kib median [min–max] (Δ vs A_sup_ct) |
|---|---|---|---|---|---|---|---|
| A_sup_ct | 5 | 12510 [12046–13196] | 71.84 [70.91–74.20] | 158.4 [135.9–179.9] | 744.4 [558.1–919.0] | 69.7 [69.0–72.0] | 8132 [8116–8164] |
| B_sup_ct | 5 | 13043 [12035–15085] (~+4.3%, p=0.349) | 70.85 [63.74–76.22] (~-1.4%, p=0.532) | 145.3 [110.8–186.6] (~-8.3%, p=0.500) | 471.9 [351.2–927.4] (~-36.6%, p=0.254) | 71.7 [70.7–72.0] (~+2.9%, p=0.103) | 8396 [8384–8428] (**+3.2%**, p=0.008) |

#### `cell clients=1 size=128 pipeline=1`

| variant | n | batches_s median [min–max] (Δ vs A_sup_ct) | p50_ns µs median [min–max] (Δ vs A_sup_ct) | p99_ns µs median [min–max] (Δ vs A_sup_ct) | p999_ns µs median [min–max] (Δ vs A_sup_ct) | cpu_pct median [min–max] (Δ vs A_sup_ct) | rss_steady_kib median [min–max] (Δ vs A_sup_ct) |
|---|---|---|---|---|---|---|---|
| A_sup_ct | 5 | 10498 [10230–10879] | 87.83 [84.25–89.82] | 203.1 [183.3–245.9] | 802.3 [678.5–927.8] | 67.7 [67.0–68.3] | 8156 [8136–8184] |
| B_sup_ct | 5 | 10544 [9663–10841] (~+0.4%, p=0.690) | 87.79 [84.70–96.12] (~-0.0%, p=0.571) | 191.5 [179.1–210.2] (~-5.7%, p=0.373) | 794.5 [560.0–935.2] (~-1.0%, p=0.659) | 69.0 [68.0–69.7] (**+1.9%**, p=0.024) | 8424 [8340–8440] (**+3.3%**, p=0.008) |

#### `cell clients=8 size=128 pipeline=32`

| variant | n | batches_s median [min–max] (Δ vs A_sup_ct) | p50_ns µs median [min–max] (Δ vs A_sup_ct) | p99_ns µs median [min–max] (Δ vs A_sup_ct) | p999_ns µs median [min–max] (Δ vs A_sup_ct) | cpu_pct median [min–max] (Δ vs A_sup_ct) | rss_steady_kib median [min–max] (Δ vs A_sup_ct) |
|---|---|---|---|---|---|---|---|
| A_sup_ct | 5 | 117937 [111258–124883] | 1995.8 [1859.7–2144.0] | 5193.4 [5099.4–5315.7] | 7884.1 [7172.1–8789.3] | 146.0 [143.7–148.0] | 8180 [8132–8200] |
| B_sup_ct | 5 | 107895 [107255–129267] (~-8.5%, p=0.524) | 2192.9 [1818.5–2242.9] (~+9.9%, p=0.341) | 4972.5 [4218.7–6310.4] (~-4.3%, p=0.786) | 7232.4 [6272.9–8297.6] (~-8.3%, p=0.159) | 146.0 [144.6–146.7] (~+0.0%, p=0.730) | 8416 [8348–8460] (**+2.9%**, p=0.008) |

#### `cell clients=8 size=1 pipeline=1`

| variant | n | batches_s median [min–max] (Δ vs A_sup_ct) | p50_ns µs median [min–max] (Δ vs A_sup_ct) | p99_ns µs median [min–max] (Δ vs A_sup_ct) | p999_ns µs median [min–max] (Δ vs A_sup_ct) | cpu_pct median [min–max] (Δ vs A_sup_ct) | rss_steady_kib median [min–max] (Δ vs A_sup_ct) |
|---|---|---|---|---|---|---|---|
| A_sup_ct | 5 | 31928 [28416–32331] | 216.2 [215.3–259.7] | 528.1 [483.1–774.0] | 4096.4 [3949.7–4188.3] | 111.6 [111.0–112.3] | 8144 [8068–8184] |
| B_sup_ct | 5 | 32400 [30787–33371] (~+1.5%, p=0.238) | 211.4 [207.2–236.0] (~-2.2%, p=0.349) | 523.5 [487.2–620.5] (~-0.9%, p=0.778) | 4154.1 [4071.1–4179.5] (~+1.4%, p=0.381) | 111.3 [111.3–112.3] (~-0.3%, p=1.000) | 8396 [8392–8412] (**+3.1%**, p=0.008) |
