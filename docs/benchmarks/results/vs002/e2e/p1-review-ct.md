
#### `cell clients=8 size=128 pipeline=32`

| variant | n | batches_s median [min–max] (Δ vs pre_ct) | p50_ns µs median [min–max] (Δ vs pre_ct) | p99_ns µs median [min–max] (Δ vs pre_ct) | p999_ns µs median [min–max] (Δ vs pre_ct) | cpu_pct median [min–max] (Δ vs pre_ct) | rss_steady_kib median [min–max] (Δ vs pre_ct) |
|---|---|---|---|---|---|---|---|
| pre_ct | 7 | 79408 [68243–82496] | 2716.7 [2599.7–3277.6] | 9244.1 [8236.6–10312.7] | 18312.2 [11576.4–22327.0] | 136.3 [119.0–138.0] | 8056 [8016–8116] |
| post_ct | 7 | 74215 [55288–77536] (~-6.5%, p=0.064) | 3003.5 [2847.4–4281.6] (~+10.6%, p=0.094) | 9711.9 [9258.0–10534.1] (~+5.1%, p=0.226) | 13468.2 [12141.7–16242.8] (~-26.5%, p=0.084) | 135.7 [131.7–139.6] (~-0.4%, p=0.742) | 8092 [8028–8120] (~+0.4%, p=0.267) |

#### `cell clients=8 size=1 pipeline=32`

| variant | n | batches_s median [min–max] (Δ vs pre_ct) | p50_ns µs median [min–max] (Δ vs pre_ct) | p99_ns µs median [min–max] (Δ vs pre_ct) | p999_ns µs median [min–max] (Δ vs pre_ct) | cpu_pct median [min–max] (Δ vs pre_ct) | rss_steady_kib median [min–max] (Δ vs pre_ct) |
|---|---|---|---|---|---|---|---|
| pre_ct | 7 | 243624 [213401–283454] | 902.8 [820.7–1000.3] | 4217.7 [2568.8–4559.4] | 5533.5 [5076.6–7275.5] | 114.7 [113.6–116.3] | 8060 [8008–8108] |
| post_ct | 7 | 219807 [205841–259867] (~-9.8%, p=0.223) | 940.9 [762.0–1050.0] (~+4.2%, p=0.561) | 4395.6 [4250.7–4541.4] (~+4.2%, p=0.108) | 5848.6 [5311.8–7090.2] (~+5.7%, p=0.671) | 113.7 [113.3–114.7] (~-0.9%, p=0.031) | 8024 [8008–8124] (~-0.4%, p=0.824) |

#### `mixed hv=16 rare_interval_us=1000 queue=64`

| variant | n | batches_s median [min–max] (Δ vs pre_ct) | p50_ns µs median [min–max] (Δ vs pre_ct) | p99_ns µs median [min–max] (Δ vs pre_ct) | p999_ns µs median [min–max] (Δ vs pre_ct) | cpu_pct median [min–max] (Δ vs pre_ct) | rss_steady_kib median [min–max] (Δ vs pre_ct) |
|---|---|---|---|---|---|---|---|
| pre_ct | 7 | – | – | – | – | 134.7 [134.3–139.7] | 8108 [8052–8148] |
| post_ct | 7 | – | – | – | – | 136.3 [134.0–140.3] (~+1.2%, p=0.600) | 8132 [8072–8156] (~+0.3%, p=0.216) |

#### `cell clients=1 size=128 pipeline=32`

| variant | n | batches_s median [min–max] (Δ vs pre_ct) | p50_ns µs median [min–max] (Δ vs pre_ct) | p99_ns µs median [min–max] (Δ vs pre_ct) | p999_ns µs median [min–max] (Δ vs pre_ct) | cpu_pct median [min–max] (Δ vs pre_ct) | rss_steady_kib median [min–max] (Δ vs pre_ct) |
|---|---|---|---|---|---|---|---|
| pre_ct | 7 | 86328 [68305–93754] | 337.5 [311.0–410.3] | 1095.8 [860.4–1210.1] | 1985.8 [1493.6–3066.8] | 137.0 [134.0–138.7] | 8048 [8028–8116] |
| post_ct | 7 | 67333 [61333–81587] (**-22.0%**, p=0.028) | 425.3 [393.8–472.0] (**+26.0%**, p=0.006) | 1273.3 [975.1–1389.8] (~+16.2%, p=0.057) | 2426.8 [2255.6–3355.6] (~+22.2%, p=0.050) | 133.3 [128.7–136.6] (**-2.7%**, p=0.008) | 8048 [8012–8080] (~+0.0%, p=0.383) |
