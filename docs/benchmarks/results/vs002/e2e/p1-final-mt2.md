
#### `mixed hv=16 rare_interval_us=1000 queue=64`

| variant | n | batches_s median [min–max] (Δ vs pre) | p50_ns µs median [min–max] (Δ vs pre) | p99_ns µs median [min–max] (Δ vs pre) | p999_ns µs median [min–max] (Δ vs pre) | rare_p50_ns µs median [min–max] (Δ vs pre) | rare_p99_ns µs median [min–max] (Δ vs pre) | rare_p999_ns µs median [min–max] (Δ vs pre) |
|---|---|---|---|---|---|---|---|---|
| pre | 7 | – | – | – | – | 3120.1 [2376.2–3466.3] | 7907.4 [6341.5–8724.7] | 9876.6 [7535.3–12981.6] |
| final | 7 | – | – | – | – | 3362.1 [2681.2–3661.7] (~+7.8%, p=0.207) | 7989.5 [6464.2–9369.3] (~+1.0%, p=0.533) | 10588.2 [8585.5–13692.7] (~+7.2%, p=0.610) |

#### `cell clients=8 size=128 pipeline=32`

| variant | n | batches_s median [min–max] (Δ vs pre) | p50_ns µs median [min–max] (Δ vs pre) | p99_ns µs median [min–max] (Δ vs pre) | p999_ns µs median [min–max] (Δ vs pre) | rare_p50_ns µs median [min–max] (Δ vs pre) | rare_p99_ns µs median [min–max] (Δ vs pre) | rare_p999_ns µs median [min–max] (Δ vs pre) |
|---|---|---|---|---|---|---|---|---|
| pre | 7 | 132282 [123761–167435] | 1734.1 [1408.2–1883.9] | 4985.1 [3845.2–5440.5] | 7849.7 [5882.0–8407.0] | – | – | – |
| final | 7 | 133188 [122554–142011] (~+0.7%, p=0.323) | 1762.8 [1640.4–1892.7] (~+1.7%, p=0.369) | 5032.3 [4731.1–5830.2] (~+0.9%, p=0.244) | 8488.0 [6551.8–17426.6] (~+8.1%, p=0.087) | – | – | – |

#### `cell clients=1 size=128 pipeline=32`

| variant | n | batches_s median [min–max] (Δ vs pre) | p50_ns µs median [min–max] (Δ vs pre) | p99_ns µs median [min–max] (Δ vs pre) | p999_ns µs median [min–max] (Δ vs pre) | rare_p50_ns µs median [min–max] (Δ vs pre) | rare_p99_ns µs median [min–max] (Δ vs pre) | rare_p999_ns µs median [min–max] (Δ vs pre) |
|---|---|---|---|---|---|---|---|---|
| pre | 7 | 53259 [50620–55920] | 551.6 [543.1–616.8] | 1300.1 [1209.1–1845.1] | 2910.5 [2163.9–4140.2] | – | – | – |
| final | 7 | 49522 [41691–53651] (**-7.0%**, p=0.032) | 599.3 [499.8–702.5] (~+8.6%, p=0.231) | 1413.9 [1276.6–1640.2] (~+8.8%, p=0.576) | 3021.2 [2451.0–5319.0] (~+3.8%, p=0.444) | – | – | – |

#### `cell clients=8 size=1 pipeline=32`

| variant | n | batches_s median [min–max] (Δ vs pre) | p50_ns µs median [min–max] (Δ vs pre) | p99_ns µs median [min–max] (Δ vs pre) | p999_ns µs median [min–max] (Δ vs pre) | rare_p50_ns µs median [min–max] (Δ vs pre) | rare_p99_ns µs median [min–max] (Δ vs pre) | rare_p999_ns µs median [min–max] (Δ vs pre) |
|---|---|---|---|---|---|---|---|---|
| pre | 7 | 457596 [361842–533636] | 502.5 [436.7–643.5] | 1601.3 [1307.1–2028.1] | 3654.3 [2913.9–4994.3] | – | – | – |
| final | 7 | 433848 [376260–515828] (~-5.2%, p=0.926) | 537.5 [450.4–618.6] (~+7.0%, p=0.922) | 1575.7 [1331.6–1881.5] (~-1.6%, p=0.671) | 3390.4 [2755.1–4872.4] (~-7.2%, p=0.604) | – | – | – |

#### `cell clients=1 size=1 pipeline=1`

| variant | n | batches_s median [min–max] (Δ vs pre) | p50_ns µs median [min–max] (Δ vs pre) | p99_ns µs median [min–max] (Δ vs pre) | p999_ns µs median [min–max] (Δ vs pre) | rare_p50_ns µs median [min–max] (Δ vs pre) | rare_p99_ns µs median [min–max] (Δ vs pre) | rare_p999_ns µs median [min–max] (Δ vs pre) |
|---|---|---|---|---|---|---|---|---|
| pre | 7 | 9633 [9007–10448] | 95.85 [87.58–101.2] | 225.7 [192.9–241.9] | 829.5 [620.1–1096.9] | – | – | – |
| final | 7 | 9552 [8634–10222] (~-0.8%, p=0.395) | 94.59 [88.53–105.9] (~-1.3%, p=0.717) | 222.2 [201.6–262.6] (~-1.6%, p=0.527) | 1002.1 [851.0–1075.0] (~+20.8%, p=0.030) | – | – | – |
