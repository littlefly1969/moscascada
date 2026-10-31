
#### `cell clients=1 size=128 pipeline=32`

| variant | n | batches_s median [min–max] (Δ vs pre_ct) | p50_ns µs median [min–max] (Δ vs pre_ct) | p99_ns µs median [min–max] (Δ vs pre_ct) | rare_p50_ns µs median [min–max] (Δ vs pre_ct) | rare_p99_ns µs median [min–max] (Δ vs pre_ct) | rare_p999_ns µs median [min–max] (Δ vs pre_ct) |
|---|---|---|---|---|---|---|---|
| pre_ct | 7 | 76332 [61078–83540] | 354.3 [325.2–423.1] | 1098.7 [978.9–1278.4] | – | – | – |
| A_ct | 7 | 66536 [56860–72359] (~-12.8%, p=0.022) | 424.6 [394.1–489.8] (**+19.8%**, p=0.006) | 1234.8 [1071.8–1326.4] (~+12.4%, p=0.071) | – | – | – |
| B_ct | 7 | 75898 [60605–85073] (~-0.6%, p=0.969) | 377.1 [320.0–533.4] (~+6.4%, p=0.402) | 1055.4 [862.0–1158.1] (~-3.9%, p=0.179) | – | – | – |

#### `mixed hv=16 rare_interval_us=1000 queue=64`

| variant | n | batches_s median [min–max] (Δ vs pre_ct) | p50_ns µs median [min–max] (Δ vs pre_ct) | p99_ns µs median [min–max] (Δ vs pre_ct) | rare_p50_ns µs median [min–max] (Δ vs pre_ct) | rare_p99_ns µs median [min–max] (Δ vs pre_ct) | rare_p999_ns µs median [min–max] (Δ vs pre_ct) |
|---|---|---|---|---|---|---|---|
| pre_ct | 7 | – | – | – | 8064.3 [7600.1–10061.7] | 19875.0 [17848.2–26798.1] | 22060.8 [19165.0–29378.3] |
| A_ct | 7 | – | – | – | 9204.2 [8823.0–12918.9] (~+14.1%, p=0.092) | 25926.6 [22824.5–33252.1] (**+30.4%**, p=0.006) | 29634.2 [24714.1–40324.6] (**+34.3%**, p=0.004) |
| B_ct | 7 | – | – | – | 8624.9 [6327.2–10195.3] (~+7.0%, p=0.668) | 21041.2 [16227.8–25076.5] (~+5.9%, p=0.681) | 25137.4 [21654.5–42029.9] (~+13.9%, p=0.139) |

#### `cell clients=8 size=128 pipeline=32`

| variant | n | batches_s median [min–max] (Δ vs pre_ct) | p50_ns µs median [min–max] (Δ vs pre_ct) | p99_ns µs median [min–max] (Δ vs pre_ct) | rare_p50_ns µs median [min–max] (Δ vs pre_ct) | rare_p99_ns µs median [min–max] (Δ vs pre_ct) | rare_p999_ns µs median [min–max] (Δ vs pre_ct) |
|---|---|---|---|---|---|---|---|
| pre_ct | 7 | 83847 [75006–95415] | 2762.8 [2446.0–2976.0] | 8430.8 [6509.1–10023.4] | – | – | – |
| A_ct | 7 | 70014 [50569–75149] (**-16.5%**, p=0.001) | 3061.6 [3021.3–4688.5] (**+10.8%**, p=0.001) | 10532.9 [8757.8–12223.2] (**+24.9%**, p=0.006) | – | – | – |
| B_ct | 7 | 81413 [78333–104088] (~-2.9%, p=0.834) | 2764.6 [2192.6–2840.7] (~+0.1%, p=0.703) | 8787.7 [7149.1–9424.7] (~+4.2%, p=0.793) | – | – | – |
