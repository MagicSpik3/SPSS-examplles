* Encoding: UTF-8.
* PURPOSE: Monthly benefit expenditure – Official Statistics Production.
* SYSTEM: PSPP-compatible legacy pipeline.

* =========================================================
  * STEP 0a: Load control variables (PSPP-safe)
* =========================================================.
GET DATA
/TYPE=TXT
/FILE='control_vars.csv'
/DELCASE=LINE
/DELIMITERS=","
/FIRSTCASE=2
/VARIABLES=
  parameter A20
value A20.

* Initialize variables to store control values.
COMPUTE min_age_n = $SYSMIS.
COMPUTE max_age_n = $SYSMIS.
COMPUTE reference_month_n = $SYSMIS.
STRING exclude_status_s (A20).

* Fix 1: Use RTRIM to handle whitespace in A20 columns.
DO IF (RTRIM(parameter) = "MIN_AGE").
COMPUTE min_age_n = NUMBER(value, F3.0).
END IF.
DO IF (RTRIM(parameter) = "MAX_AGE").
COMPUTE max_age_n = NUMBER(value, F3.0).
END IF.
DO IF (RTRIM(parameter) = "REFERENCE_MONTH").
COMPUTE reference_month_n = NUMBER(value, F6.0).
END IF.
DO IF (RTRIM(parameter) = "EXCLUDE_STATUS").
COMPUTE exclude_status_s = value.
END IF.
EXECUTE.

* Collapse to a single row containing all control values.
SELECT IF NOT MISSING(min_age_n).
COMPUTE join_key = 1.
EXECUTE.

SAVE OUTFILE='control_values.sav'.

* =========================================================
  * STEP 0b: Load benefit rates (Pre-load to .sav)
* =========================================================.
GET DATA
/TYPE=TXT
/FILE='benefit_rates.csv'
/ARRANGEMENT=DELIMITED
/DELCASE=LINE
/FIRSTCASE=2
/DELIMITERS=","
/VARIABLES=
  benefit_type A5
weekly_rate    F6.2.

SORT CASES BY benefit_type.
SAVE OUTFILE='benefit_rates.sav'.

* =========================================================
  * STEP 1: Load claims data (Main Dataset)
* =========================================================.
GET DATA
/TYPE=TXT
/FILE='claims_data.csv'
/DELCASE=LINE
/DELIMITERS=","
/FIRSTCASE=2
/VARIABLES=
  claim_id F8.0
person_id F8.0
dob A8
claim_start A8
claim_end A8
benefit_type A5
region A20
status A10.

COMPUTE join_key = 1.
EXECUTE.

* =========================================================
  * STEP 1b: Merge control values into claims
* =========================================================.
MATCH FILES
/FILE=*
  /TABLE='control_values.sav'
/BY join_key.
EXECUTE.

* =========================================================
  * STEP 2: Convert string dates to PSPP dates
* =========================================================.
COMPUTE dob_num = NUMBER(dob, F8.0).
COMPUTE claim_start_num = NUMBER(claim_start, F8.0).
COMPUTE claim_end_num = NUMBER(claim_end, F8.0).

COMPUTE dob_date = DATE.MDY(
  TRUNC(MOD(dob_num,10000)/100),
  MOD(dob_num,100),
  TRUNC(dob_num/10000)
).
COMPUTE claim_start_date = DATE.MDY(
  TRUNC(MOD(claim_start_num,10000)/100),
  MOD(claim_start_num,100),
  TRUNC(claim_start_num/10000)
).
COMPUTE claim_end_date = DATE.MDY(
  TRUNC(MOD(claim_end_num,10000)/100),
  MOD(claim_end_num,100),
  TRUNC(claim_end_num/10000)
).
FORMATS dob_date claim_start_date claim_end_date (DATE11).

* =========================================================
  * STEP 3: Age eligibility
* =========================================================.
* Fix 2: Divide by (365.25 * 86400) because dates are in seconds.
COMPUTE age_years = TRUNC((claim_start_date - dob_date) / (365.25 * 86400)).

COMPUTE age_valid = 1.
IF (age_years < min_age_n) age_valid = 0.
IF (age_years > max_age_n) age_valid = 0.

* =========================================================
  * STEP 4: Status exclusion
* =========================================================.
COMPUTE status_valid = 1.
IF (RTRIM(status) = RTRIM(exclude_status_s)) status_valid = 0.

* =========================================================
  * STEP 5: Calculate eligible days in reference month
* =========================================================.
COMPUTE ref_year  = TRUNC(reference_month_n / 100).
COMPUTE ref_month = MOD(reference_month_n, 100).

COMPUTE month_start = DATE.MDY(ref_month, 1, ref_year).
COMPUTE month_end = DATE.MDY(ref_month + 1, 1, ref_year) - 1.

COMPUTE eligible_start = MAX(claim_start_date, month_start).
COMPUTE eligible_end   = MIN(claim_end_date, month_end).

* Fix 3: Divide by 86400 to convert seconds to days.
COMPUTE eligible_days = (eligible_end - eligible_start) / 86400 + 1.
IF (eligible_days < 0) eligible_days = 0.

* =========================================================
  * STEP 6: Merge benefit rates
* =========================================================.
SORT CASES BY benefit_type.
MATCH FILES
/FILE=*
  /TABLE='benefit_rates.sav'
/BY benefit_type.
EXECUTE.

* =========================================================
  * STEP 7: Payment calculation
* =========================================================.
COMPUTE daily_rate = weekly_rate / 7.
COMPUTE payment_amount = eligible_days * daily_rate.

* =========================================================
  * STEP 8: Apply business rules
* =========================================================.
SELECT IF (age_valid = 1 AND status_valid = 1 AND eligible_days > 0).

* =========================================================
  * STEP 9: Aggregate outputs
* =========================================================.
AGGREGATE
/OUTFILE=*
  /BREAK=benefit_type region
/total_paid = SUM(payment_amount).

* =========================================================
  * STEP 10: Export results
* =========================================================.
* Fix 4: Use /OUTFILE instead of /FILE.
SAVE TRANSLATE
/TYPE=CSV
/OUTFILE='benefit_monthly_summary.csv'
/REPLACE
/FIELDNAMES.

LIST.
