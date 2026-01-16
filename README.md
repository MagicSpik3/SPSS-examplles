# SPSS-examplles
SPSS
# SPSS → State Machine Pipeline Test Case

This test case validates the SPSS-to-State-Machine conversion pipeline using a realistic data processing workflow that is fully compatible with PSPP on Linux.

## Overview

The pipeline simulates a civil registry processing system that:

1. Loads a dataset of people with:
   - date of registration
   - date of death
   - region code
   - income

2. Converts string dates (`YYYYMMDD`) into numeric values and constructs real SPSS date objects.

3. Computes the delay (in days) between registration and death.

4. Filters out invalid records where the delay is negative.

5. Derives the month of death.

6. Joins the data with a region lookup table.

7. Categorises individuals into income bands:
   - LOW (< 20,000)
   - MEDIUM (20,000–39,999)
   - HIGH (>= 40,000)

8. Aggregates results by:
   - region name
   - income band

9. Outputs:
   - total number of deaths
   - average delay in days

The final result is written to `final_output.csv`.

## Files

| File | Description |
|------|-------------|
| `people.csv` | Main dataset of individuals |
| `regions.csv` | Lookup table mapping region codes to names |
| `pipeline.sps` | SPSS/PSPP syntax pipeline |
| `final_output.csv` | Generated output |

## Purpose

This scenario is designed to exercise:

- numeric casting
- date construction
- arithmetic on dates
- conditional logic (IF)
- filtering (SELECT IF)
- dataset joins (MATCH FILES)
- string variable creation
- aggregation
- execution barriers (EXECUTE)
- file input and output

It is suitable for:

- state machine extraction
- semantic operation detection
- R code generation
- regression testing
- pipeline validation

## Running with PSPP

```bash
pspp pipeline.sps
