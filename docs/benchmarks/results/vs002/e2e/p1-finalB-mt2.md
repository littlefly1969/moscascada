
#### `mixed hv=16 rare_interval_us=1000 queue=64`

| variant | n | batches_s median [min–max] (Δ vs pre) | p50_ns µs median [min–max] (Δ vs pre) | p99_ns µs median [min–max] (Δ vs pre) | p999_ns µs median [min–max] (Δ vs pre) | rare_p50_ns µs median [min–max] (Δ vs pre) | rare_p99_ns µs median [min–max] (Δ vs pre) | rare_p999_ns µs median [min–max] (Δ vs pre) |
|---|---|---|---|---|---|---|---|---|
| pre | 7 | – | – | – | – | 2575.6 [2258.2–3219.9] | 7225.8 [6501.2–8783.9] | 10310.3 [8950.1–19128.0] |
| final | 7 | – | – | – | – | 2457.6 [1890.4–3054.4] (~-4.6%, p=0.603) | 7803.8 [6008.6–8315.9] (~+8.0%, p=0.808) | 10903.1 [7559.2–46867.3] (~+5.7%, p=0.421) |

#### `cell clients=1 size=1 pipeline=1`

| variant | n | batches_s median [min–max] (Δ vs pre) | p50_ns µs median [min–max] (Δ vs pre) | p99_ns µs median [min–max] (Δ vs pre) | p999_ns µs median [min–max] (Δ vs pre) | rare_p50_ns µs median [min–max] (Δ vs pre) | rare_p99_ns µs median [min–max] (Δ vs pre) | rare_p999_ns µs median [min–max] (Δ vs pre) |
|---|---|---|---|---|---|---|---|---|
| pre | 7 | 9808 [9515–10954] | 92.90 [81.73–96.45] | 224.6 [197.9–260.7] | 675.2 [506.7–1220.3] | – | – | – |
| final | 7 | 9707 [8976–10463] (~-1.0%, p=0.417) | 93.78 [87.91–101.1] (~+0.9%, p=0.283) | 222.2 [190.2–254.7] (~-1.1%, p=0.933) | 688.7 [467.6–796.9] (~+2.0%, p=0.312) | – | – | – |

#### `cell clients=1 size=128 pipeline=32`

| variant | n | batches_s median [min–max] (Δ vs pre) | p50_ns µs median [min–max] (Δ vs pre) | p99_ns µs median [min–max] (Δ vs pre) | p999_ns µs median [min–max] (Δ vs pre) | rare_p50_ns µs median [min–max] (Δ vs pre) | rare_p99_ns µs median [min–max] (Δ vs pre) | rare_p999_ns µs median [min–max] (Δ vs pre) |
|---|---|---|---|---|---|---|---|---|
| pre | 7 | 57731 [53246–64879] | 510.5 [415.8–551.2] | 1358.5 [1272.8–1850.7] | 3386.3 [2535.3–4518.4] | – | – | – |
| final | 7 | 62564 [52095–65070] (~+8.4%, p=0.243) | 448.6 [427.5–558.2] (~-12.1%, p=0.291) | 1275.3 [1226.7–1490.3] (~-6.1%, p=0.214) | 3803.2 [1980.1–4053.9] (~+12.3%, p=0.857) | – | – | – |

#### `cell clients=8 size=1 pipeline=32`

| variant | n | batches_s median [min–max] (Δ vs pre) | p50_ns µs median [min–max] (Δ vs pre) | p99_ns µs median [min–max] (Δ vs pre) | p999_ns µs median [min–max] (Δ vs pre) | rare_p50_ns µs median [min–max] (Δ vs pre) | rare_p99_ns µs median [min–max] (Δ vs pre) | rare_p999_ns µs median [min–max] (Δ vs pre) |
|---|---|---|---|---|---|---|---|---|
| pre | 7 | 440970 [416662–482411] | 516.8 [483.5–553.7] | 1533.6 [1430.5–1744.0] | 3267.0 [3136.1–4234.3] | – | – | – |
| final | 7 | 433530 [370298–453118] (~-1.7%, p=0.110) | 526.5 [518.5–633.5] (~+1.9%, p=0.135) | 1687.2 [1467.3–1824.8] (~+10.0%, p=0.114) | 3930.0 [3359.9–4620.1] (~+20.3%, p=0.115) | – | – | – |

#### `cell clients=8 size=128 pipeline=32`

| variant | n | batches_s median [min–max] (Δ vs pre) | p50_ns µs median [min–max] (Δ vs pre) | p99_ns µs median [min–max] (Δ vs pre) | p999_ns µs median [min–max] (Δ vs pre) | rare_p50_ns µs median [min–max] (Δ vs pre) | rare_p99_ns µs median [min–max] (Δ vs pre) | rare_p999_ns µs median [min–max] (Δ vs pre) |
|---|---|---|---|---|---|---|---|---|
| pre | 7 | 145190 [126436–153354] | 1608.5 [1500.9–1857.2] | 4668.6 [4310.7–5864.0] | 7651.6 [6274.0–21088.5] | – | – | – |
| final | 7 | 140001 [127626–162456] (~-3.6%, p=0.941) | 1662.8 [1460.1–1839.7] (~+3.4%, p=0.943) | 4856.8 [3969.9–6153.9] (~+4.0%, p=0.884) | 8383.4 [5912.1–30760.4] (~+9.6%, p=0.524) | – | – | – |
