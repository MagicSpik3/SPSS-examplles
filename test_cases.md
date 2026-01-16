That diagnosis is exactly right 👍 — your extractor and DAG builder are already quite strong. What you’re missing is:

* **ground-truth SPSS semantics**, and
* a **systematic test suite** to lock your IR design.

Below is a ready-to-use **SPSS → State Machine test suite blueprint** with concrete programs, expected operations, and what each case validates.

You can turn these directly into golden tests.

---

# How to use these test cases

For each test:

1. Feed the SPSS file to your parser.
2. Generate the state machine.
3. Compare against the expected **operation types + key params** (not variable IDs).
4. Assert:

   * correct ops exist
   * correct dependencies
   * correct semantic fields populated
   * no `generic` fallbacks

---

# Test Case 1 – Load + Sort + Save

## SPSS

```spss
GET DATA
  /TYPE=TXT
  /FILE='a.csv'
  /DELIMITERS=','.

SORT CASES BY id.

SAVE OUTFILE='b.sav'.
```

## Purpose

Validates:

* LOAD
* SORT_ROWS
* SAVE_BINARY
* file vs dataset separation

## Expected State Machine (semantic)

```yaml
LOAD_CSV(filename="a.csv") -> ds1
SORT_ROWS(input=ds1, keys=["id"]) -> ds2
SAVE_BINARY(input=ds2, filename="b.sav")
```

---

# Test Case 2 – Simple compute

## SPSS

```spss
COMPUTE x2 = x * 2.
EXECUTE.
```

## Purpose

* Expression parsing
* Arithmetic AST
* BATCH_COMPUTE

## Expected

```yaml
BATCH_COMPUTE:
  target: x2
  expr:
    op: "*"
    left: column(x)
    right: literal(2)
```

---

# Test Case 3 – Multiple computes

```spss
COMPUTE a = x + y.
COMPUTE b = a * 10.
EXECUTE.
```

## Expected

Two compute entries in one batch or two ops.

---

# Test Case 4 – IF conditional assignment

```spss
STRING band (A10).
IF (income < 20000) band = "LOW".
IF (income >= 20000 AND income < 40000) band = "MED".
IF (income >= 40000) band = "HIGH".
EXECUTE.
```

## Purpose

Critical: conditional derive

## Expected IR

```yaml
DERIVE_COLUMN:
  target: band
  cases:
    - when: income < 20000
      value: "LOW"
    - when: income >= 20000 AND income < 40000
      value: "MED"
    - when: income >= 40000
      value: "HIGH"
```

---

# Test Case 5 – SELECT IF (filter)

```spss
SELECT IF age >= 18.
EXECUTE.
```

## Expected

```yaml
FILTER_ROWS(condition: age >= 18)
```

---

# Test Case 6 – Dates

```spss
COMPUTE y = TRUNC(date_num / 10000).
COMPUTE m = TRUNC((date_num - y*10000)/100).
COMPUTE d = date_num - y*10000 - m*100.
COMPUTE dt = DATE.MDY(m, d, y).
EXECUTE.
```

## Expected

Expression AST with:

* TRUNC
* arithmetic
* DATE.MDY call

---

# Test Case 7 – Aggregate

```spss
AGGREGATE
  /OUTFILE=*
  /BREAK=region
  /N_PEOPLE = N
  /AVG_AGE = MEAN(age).
```

## Expected

```yaml
AGGREGATE:
  break: ["region"]
  aggregations:
    - target: N_PEOPLE
      func: COUNT
    - target: AVG_AGE
      func: MEAN
      column: age
```

---

# Test Case 8 – Join (MATCH FILES)

```spss
MATCH FILES
  /FILE=people.sav
  /TABLE=regions.sav
  /BY region_id.
EXECUTE.
```

## Expected

```yaml
JOIN:
  left: people
  right: regions
  by: ["region_id"]
  type: LEFT
```

---

# Test Case 9 – Full pipeline (realistic)

This mirrors your example but simplified.

## Files

### people.csv

```csv
id,region,income,reg_date,death_date
1,10,15000,20200101,20200110
2,10,45000,20200105,20200120
3,20,30000,20200103,20200102
```

### regions.csv

```csv
region,region_name
10,North
20,South
```

## SPSS

```spss
GET DATA /TYPE=TXT /FILE='regions.csv' /DELIMITERS=','.
SORT CASES BY region.
SAVE OUTFILE='tmp_regions.sav'.

GET DATA /TYPE=TXT /FILE='people.csv' /DELIMITERS=','.

COMPUTE ry = TRUNC(reg_date / 10000).
COMPUTE rm = TRUNC((reg_date - ry*10000)/100).
COMPUTE rd = reg_date - ry*10000 - rm*100.
COMPUTE dy = TRUNC(death_date / 10000).
COMPUTE dm = TRUNC((death_date - dy*10000)/100).
COMPUTE dd = death_date - dy*10000 - dm*100.

COMPUTE reg_dt = DATE.MDY(rm, rd, ry).
COMPUTE death_dt = DATE.MDY(dm, dd, dy).
COMPUTE delay = death_dt - reg_dt.

SELECT IF delay >= 0.

MATCH FILES
  /FILE=*
  /TABLE='tmp_regions.sav'
  /BY region.

STRING band (A10).
IF (income < 20000) band = "LOW".
IF (income >= 20000 AND income < 40000) band = "MED".
IF (income >= 40000) band = "HIGH".

AGGREGATE
  /OUTFILE=*
  /BREAK=region_name band
  /N_DEATHS = N
  /AVG_DELAY = MEAN(delay).

SAVE OUTFILE='final.csv'.
```

---

## Expected operations (semantic order)

| Op                                 | Type |
| ---------------------------------- | ---- |
| LOAD_CSV regions                   |      |
| SORT_ROWS                          |      |
| SAVE_BINARY                        |      |
| LOAD_CSV people                    |      |
| BATCH_COMPUTE (date split)         |      |
| BATCH_COMPUTE (date build + delay) |      |
| FILTER_ROWS                        |      |
| JOIN (LEFT)                        |      |
| DERIVE_COLUMN band                 |      |
| AGGREGATE                          |      |
| SAVE_CSV                           |      |

No `generic` operations.

---

# Test harness structure suggestion

Store each test as:

```
tests/
  test01_load_sort_save/
    program.sps
    expected_ir.json
  test02_compute/
    program.sps
    expected_ir.json
  ...
```

Then:

```python
ir = parse_spss(program)
normalize(ir)
assert ir == expected_ir
```

Normalization removes:

* generated IDs
* dataset names

---

# Bonus: SPSS semantics cheat sheet (for you)

| SPSS        | Meaning                 |
| ----------- | ----------------------- |
| COMPUTE     | create/overwrite column |
| IF          | conditional assignment  |
| SELECT IF   | filter rows             |
| STRING      | declare string column   |
| MATCH FILES | left join               |
| AGGREGATE   | group by                |
| EXECUTE     | barrier                 |
| N           | count                   |
| MEAN        | average                 |
| TRUNC       | truncate                |
| DATE.MDY    | date constructor        |

---

# Final recommendation

Implement these test cases **before touching your codegen again**.

They will:

* define what your state machine must represent
* eliminate `generic` fallbacks
* guide your IR redesign
* make R generation trivial

---

If you'd like, I can next:

* provide JSON schemas for expected IR
* generate expected IR for all tests
* help design a minimal expression AST
* or build a test-case generator script

You’re building something genuinely advanced – you just need semantic guardrails now.
