# My thoughts walkthrough writing the code -

## What this task is all about:
While obviously related to the ability to write healthy code, think about performance and resource utilization, the ability to think "algorithmically", and so on with what I expect to be expected, I find the idea of this all a little mhh..

### Why Mhh..
1. There are already 7 regex methods in the DataFusion API.
2. It takes ChatGPT 1 minute to generate a full solution, and a few more pushes will add many fancy improvements. This will make the ability to actually give an observation about a candidate almost impossible, and you'll need an Einstein to get something out of it.
3. I expect, if I were recruiting for a small team for a senior/staff engineer for a low-level Rust position, that I would expect them to take the lead on leveling up the performance. When they are the 10th engineer, they need to have a little more than the ability to write nice, efficient code; there are piles of those at the top companies, just pay enough.

_Obviously - I might be completely wrong and over-engineering my thoughts, but if I would need to choose the 10th engineer in my R&D, I would expect a bit more than machine understanding, or, as said, Einstein._

### So my understanding:
1. My assumption, based on the brief explanation "you will face a similar challenge to what you'll face here," is that I should approach the issue like I'm implementing it for the service.
2. In addition, while not exactly the same, many regex operations already exist in DataFusion, so my obvious question is why there's a need to implement another one. And if the answer is because it differs greatly and gives extra important functionality, why wasn't it implemented yet by the community?

_This, and probably my tendency to overthink, and my habits as an OSS community leader, and the user facing figure in my team, made me assume that I want to solve a business-related problem, along with a logical, performance oriented problem_.

### What yes:

# My Thought Process While Writing the Code

## What This Task Is Really About
While it’s obviously about writing healthy code and thinking about performance, resource use, and algorithmic choices, I still find the premise a bit… *hmm*.

### Why *Hmm…*
1. There are already seven regex-related methods in the DataFusion API.
2. ChatGPT can spit out a working solution in about a minute, and with two more nudges it’ll add fancy bells and whistles. That makes it hard to evaluate real engineering judgment; you’d practically need an Einstein to extract meaningful signal from that setup.
3. If I were hiring the tenth engineer for a small, senior/staff, low-level Rust team, I’d expect them to lead on performance architecture—not just write efficient code. Plenty of people can write tidy, fast code at top companies; the bar here should include systems thinking and product impact.

*I might be wrong and overthinking it. But if I’m choosing the tenth engineer in my R&D, I want more than machine-level understanding—or someone who has to be Einstein to stand out.*

### My Interpretation of the Task
1. Given the brief—“you’ll face a similar challenge to what you’ll face here”—I’m going to treat this like a production implementation, not a coding puzzle.
2. DataFusion already exposes multiple regex operations. So why add another? If this new function is truly different or critical, why hasn’t the community landed it yet? That question matters for scope, semantics, and maintenance.

This thinking—plus my tendency to overanalyze and my role as an OSS community lead and user‑facing engineer—nudges me to frame this as both a product problem and a performance/systems problem.

### What Yes (Plan of Attack)
- **Clarify the delta:** Precisely define how this regex op differs from existing DataFusion functions (semantics, null handling, capture groups, UTF‑8/UTF‑16 boundaries, performance guarantees).
- **Agree on constraints:** Input types, behavior on invalid patterns, max capture groups, memory bounds, and streaming vs. batch semantics.
- **Design for speed:** Columnar-friendly implementation, minimal allocations, SIMD‑aware scans where relevant, and predictable memory growth.
- **Integrate like it’s real:** Clear API surface, good error messages, metrics/hooks for observability, and doc examples mirroring real workloads.
- **Benchmark honestly:** Micro + end‑to‑end: throughput (MB/s), p50/p99 latency per batch, peak RSS, and regressions vs. existing regex ops.

If the above checks out, I’ll implement it like I’m shipping it in production—because that’s the spirit I’m reading into this exercise.
# My thoughts walkthrough writing the code -

## What this task is all about:
While this clearly tests writing healthy code and thinking about performance, resource use, and algorithmic choices, I still find the whole premise a bit… *mhh*.

### Why Mhh..
1. DataFusion already exposes several regex methods.
2. ChatGPT can produce a working solution in about a minute, and with a couple more prompts it can layer on “smart” improvements. That makes it hard to actually evaluate a candidate’s engineering judgment—you’d need an Einstein to extract meaningful signal from that setup.
3. If I were hiring the tenth engineer for a small, senior/staff, low‑level Rust team, I’d expect leadership on performance and system design—not just neat, efficient code. Plenty of engineers can write fast code at top companies; the differentiator should be systems thinking and product impact.

*I may be wrong and over‑engineering my thoughts, but if I had to pick the tenth engineer in my R&D org, I’d want more than machine‑level understanding—or someone who has to be Einstein to stand out.*

### So my understanding:
1. Based on “you will get a similar challenge to what you’ll face here,” I should treat this like a production implementation, not a puzzle.
2. Since many regex operations already exist in DataFusion, the obvious question is why another one is needed. If it’s materially different and adds important functionality, why hasn’t the community implemented it yet?

This, plus my tendency to overthink—and my habits as an OSS community leader and the user‑facing person on my team—pushes me to frame this as both a business‑driven problem and a performance‑oriented engineering problem.

## What yes, still thinking aloud:

### Full API compatibility with spark
- The service promising plug and play compatibility with spark as one of the leading selling points, and if you sell "zero code changes" the code need to accept the same input, behave the same, and return the same output. But faster and less resource consuming.
So first constraint - "never break the user's space API".

## Spark `regexp_extract` semantics

**Spark signature**

- Scala/Java (DataFrame API): `regexp_extract(str: Column, pattern: String, groupIdx: Int): Column`
- SQL: `regexp_extract(str, pattern [, groupIdx])` (default `groupIdx = 1`)

**Behavior (to mirror exactly):**
- Returns the **first match** for the requested group index.
  - `groupIdx = 0` → entire match
  - `groupIdx ≥ 1` → that capturing group
- If the pattern **does not match** or the group **didn’t participate**, return **empty string** (not `NULL`).
- If `str` is **NULL**, the result is **NULL** for that row.
- `pattern` may be a **scalar** (broadcast) or an **array column**.
- On invalid regex **fail fast** with a clear error message.
- Uses Java-style regex semantics.

```rust
// wrong signature, not datafusion-ish and force non-default values
fn regexp_extract(
    input: &ColumnarValue,      // string column or scalar
    pattern: &ColumnarValue,    // regex pattern (string column or scalar)
    group_idx: &ColumnarValue,  // capture group index (int64 scalar)
) -> Result<ColumnarValue>

// correct signature, datafusion-ish and default values
fn regexp_extract(args: &[ColumnarValue]) -> Result<ColumnarValue>
```

### Challenges - first thoughts:
- Regex in rust is not the same as in Java, it doesn't support lookahead, lookbehind, backreferences, and some other features. We need to use alternatives to support the Java semantics.
- Regex, in general, is expensive and not simd friendly. We need to optimize it.
- Input may be a scalar or an array column. We need to handle both cases, and each case has its own challenges.
- The Rust-way of doing things not allowing default values. While theres the option to use None, when theres a struct with default value, the behavior the should be used in rust is using a builder pattern and use `with_`, and when not passed can use None as default, but None as a parameter is not for having a default value replacing it. Here we need to have a default value for the group_idx.
---

### Mitigations:
- For regex support:
1. Easy way - add docs and constraints for the regex pattern, and let the user know that the regex pattern is not supported - not the point, cant sell the product this way.
2. Less easy way - use a regex crate for most cases, with fallback engine (fancy-regex or optional PCRE2 via FFI) for the cases that are not supported. (fancy-regex? PCRE2? to be determined)
3. Going even more crazy - use a custom regex engine that is optimized for the use case - bad idea, implementation cost and maintenance cost are too high, higher than the cost of the feature.
*verdict: B*

- For performance optimization, first things that comes to mind:
1. For scalar input, precompile the regex pattern once - not questionable, should be done.
2. For columnar input:
   1. Precompile the regex pattern once per batch.
   2. Precompile the regex patterns for each row immediately.

*lets add a point for the trade-off between the two*
Enhancement: use a small cache for patterns - match against the cache first, if not found, compile the pattern and add to the cache. Pattern matching is cheaper then regex compilation.
Feasibility - not complex.
*verdict: A*
** while verdict is A, questions need to asked -
Does regex patterns repeat across rows frequently? We might creating extra code and cache misses, while not getting any performance benefit out of the lab.

OPEN QUESTION:
Do we want for large batches and use threading to compile the patterns?

lets write the sketch of the code and benchmark it.
Then reassess and continue with the implementation.

## v0.1 is a brute-force baseline:
	•	Compile regex once for scalar patterns, per-row for arrays.
	•	Return NULL on any NULL input.
	•	Reject non-Rust-regex patterns with error.
	•	If idx < 0, error “must be ≥ 0.”
	•	If idx exceeds capture groups, error “index out of range.”

### Test coverage
1. extracted the spark tests and created a contract tests to verify the behavior.
2. In contract tests, added anything that is clear requirement from the docs.
See [tests/spark_contract.rs](tests/spark_contract.rs)
3. Added general test coverage - see [tests/regexp_extract_cases.md](tests/regexp_extract_cases.md).
- *note: [a\\d]{0, 2} is not allowed in spark as not a valid java regex, but it is allowed in rust regex, decided to allow it, and removed the contract that was failing on it, it is open for discussion*
*At this point the test all pass, beside the non valid rust regex:*
    _spark_backreference_repeats_full_match_
    _spark_default_index_defaults_to_group_one_
    _spark_lookbehind_returns_captured_digits_
    _spark_named_backreference_repeats_match_
    _spark_negative_lookbehind_preserves_full_match_
    _spark_positive_lookahead_selects_alphanumeric_word_

## v0.2 accepting two or three arguments, and defaulting the group_idx to 1.
### Test status:
  Tests failing now are:
    _spark_lookbehind_returns_captured_digits_
    _spark_named_backreference_repeats_match_
    _spark_negative_lookbehind_preserves_full_match_
    _spark_positive_lookahead_selects_alphanumeric_word_

## Benchmarking
