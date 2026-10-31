
#### `mixed hv=16 rare_interval_us=1000 queue=64`

| variant | n | batches_s median [min–max] (Δ vs pre_ct) | p50_ns µs median [min–max] (Δ vs pre_ct) | p99_ns µs median [min–max] (Δ vs pre_ct) | p999_ns µs median [min–max] (Δ vs pre_ct) | rare_p50_ns µs median [min–max] (Δ vs pre_ct) | rare_p99_ns µs median [min–max] (Δ vs pre_ct) | rare_p999_ns µs median [min–max] (Δ vs pre_ct) |
|---|---|---|---|---|---|---|---|---|
| pre_ct | 7 | – | – | – | – | 8826.2 [7045.8–11038.9] | 20075.1 [17965.8–23606.6] | 23277.9 [20486.7–35677.0] |
| final_ct | 7 | – | – | – | – | 9100.6 [8040.8–11123.8] (~+3.1%, p=0.509) | 23911.1 [23365.7–27815.3] (**+19.1%**, p=0.002) | 29877.5 [25049.0–32804.5] (~+28.4%, p=0.200) |

#### `cell clients=8 size=128 pipeline=32`

| variant | n | batches_s median [min–max] (Δ vs pre_ct) | p50_ns µs median [min–max] (Δ vs pre_ct) | p99_ns µs median [min–max] (Δ vs pre_ct) | p999_ns µs median [min–max] (Δ vs pre_ct) | rare_p50_ns µs median [min–max] (Δ vs pre_ct) | rare_p99_ns µs median [min–max] (Δ vs pre_ct) | rare_p999_ns µs median [min–max] (Δ vs pre_ct) |
|---|---|---|---|---|---|---|---|---|
| pre_ct | 7 | 80690 [71257–92535] | 2805.2 [2401.8–3307.5] | 9056.9 [8063.1–10135.2] | 13785.8 [11553.0–25288.4] | – | – | – |
| final_ct | 7 | 72055 [50743–79415] (~-10.7%, p=0.019) | 3135.3 [2899.9–4712.2] (~+11.8%, p=0.020) | 9783.9 [7809.6–12674.5] (~+8.0%, p=0.070) | 12908.1 [11243.5–17089.8] (~-6.4%, p=0.378) | – | – | – |

#### `cell clients=1 size=128 pipeline=32`

| variant | n | batches_s median [min–max] (Δ vs pre_ct) | p50_ns µs median [min–max] (Δ vs pre_ct) | p99_ns µs median [min–max] (Δ vs pre_ct) | p999_ns µs median [min–max] (Δ vs pre_ct) | rare_p50_ns µs median [min–max] (Δ vs pre_ct) | rare_p99_ns µs median [min–max] (Δ vs pre_ct) | rare_p999_ns µs median [min–max] (Δ vs pre_ct) |
|---|---|---|---|---|---|---|---|---|
| pre_ct | 7 | 74234 [66259–88979] | 349.3 [312.8–382.4] | 1106.6 [921.1–1253.4] | 2300.2 [1270.8–3254.2] | – | – | – |
| final_ct | 7 | 61949 [56616–80843] (**-16.5%**, p=0.024) | 444.3 [362.7–494.1] (**+27.2%**, p=0.002) | 1283.2 [1107.0–1472.3] (**+16.0%**, p=0.031) | 2629.8 [1891.3–3818.4] (~+14.3%, p=0.255) | – | – | – |

#### `cell clients=8 size=1 pipeline=32`

| variant | n | batches_s median [min–max] (Δ vs pre_ct) | p50_ns µs median [min–max] (Δ vs pre_ct) | p99_ns µs median [min–max] (Δ vs pre_ct) | p999_ns µs median [min–max] (Δ vs pre_ct) | rare_p50_ns µs median [min–max] (Δ vs pre_ct) | rare_p99_ns µs median [min–max] (Δ vs pre_ct) | rare_p999_ns µs median [min–max] (Δ vs pre_ct) |
|---|---|---|---|---|---|---|---|---|
| pre_ct | 7 | 234663 [207048–248309] | 906.3 [832.8–1051.1] | 4297.8 [4012.4–4660.6] | 5682.9 [5362.9–5789.9] | – | – | – |
| final_ct | 7 | 210929 [187882–251268] (~-10.1%, p=0.240) | 1020.8 [829.9–1153.0] (~+12.6%, p=0.212) | 4313.0 [3741.3–4641.5] (~+0.4%, p=0.755) | 6013.5 [5010.3–7371.3] (~+5.8%, p=0.256) | – | – | – |

#### `cell clients=1 size=1 pipeline=1`

| variant | n | batches_s median [min–max] (Δ vs pre_ct) | p50_ns µs median [min–max] (Δ vs pre_ct) | p99_ns µs median [min–max] (Δ vs pre_ct) | p999_ns µs median [min–max] (Δ vs pre_ct) | rare_p50_ns µs median [min–max] (Δ vs pre_ct) | rare_p99_ns µs median [min–max] (Δ vs pre_ct) | rare_p999_ns µs median [min–max] (Δ vs pre_ct) |
|---|---|---|---|---|---|---|---|---|
| pre_ct | 7 | 8937 [8215–9224] | 103.6 [98.58–111.1] | 227.5 [195.2–250.6] | 888.2 [556.1–935.6] | – | – | – |
| final_ct | 7 | 8581 [8148–9023] (~-4.0%, p=0.285) | 107.8 [102.7–115.0] (~+4.1%, p=0.123) | 237.0 [203.8–248.5] (~+4.2%, p=0.558) | 938.6 [612.2–1165.5] (~+5.7%, p=0.246) | – | – | – |
