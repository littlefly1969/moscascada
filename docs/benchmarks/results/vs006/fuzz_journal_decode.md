# Fuzz run 2026-11-01 VS-006 (cargo-fuzz 0.13.2, rustc 1.101.0-nightly (f9ceb4a4d 2026-10-31), -O, 300 s)

```
journal_decode: #1278041	DONE   cov: 803 ft: 1355 corp: 282/38Kb lim: 4096 exec/s: 4245 rss: 89Mb
journal_decode: Done 1278041 runs in 301 second(s)
crash artifacts (fuzz/artifacts/journal_decode): 0
```

Target: decode_op / decode_checkpoint / decode_source_times on arbitrary bytes; every decoded value must re-encode to the canonical bytes (no panic, no non-canonical acceptance). CI runs it as a 60 s smoke step.
