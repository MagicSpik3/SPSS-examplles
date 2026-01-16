SET MESSAGES=ON.
SET ERRORS=ON.
SET PRINTBACK=ON.

* ----------------------------------------------------------------
* STEP 1: PREPARE REGIONS
* ----------------------------------------------------------------.
TITLE "--- DEBUG: LOADING REGIONS ---".

GET DATA
  /TYPE=TXT
  /FILE='regions.csv'
  /DELCASE=LINE
  /DELIMITERS=','
  /QUALIFIER='"'
  /ARRANGEMENT=DELIMITED
  /FIRSTCASE=2
  /VARIABLES=
    region_code A1
    region_name A20.
EXECUTE.

* DEBUG: Show us the first 5 regions.
LIST VARIABLES=ALL /CASES=5.

SORT CASES BY region_code.
SAVE OUTFILE='temp_regions.sav'.
EXECUTE.

* ----------------------------------------------------------------
* STEP 2: LOAD PEOPLE
* ----------------------------------------------------------------.
TITLE "--- DEBUG: LOADING PEOPLE ---".

GET DATA
  /TYPE=TXT
  /FILE='people.csv'
  /DELCASE=LINE
  /DELIMITERS=','
  /QUALIFIER='"'
  /ARRANGEMENT=DELIMITED
  /FIRSTCASE=2
  /VARIABLES=
    person_id F8.0
    dor A8
    dod A8
    region_code A1
    income F8.0.
EXECUTE.

* DEBUG: Check if we actually loaded rows.
DESCRIPTIVES VARIABLES=income.

* ----------------------------------------------------------------
* STEP 3: DATE MATH
* ----------------------------------------------------------------.
TITLE "--- DEBUG: DATE CALCULATIONS ---".

* Convert strings 'YYYYMMDD' to numbers.
COMPUTE dor_num = NUMBER(dor, F8.0).
COMPUTE dod_num = NUMBER(dod, F8.0).

* Split dates.
COMPUTE dor_y = TRUNC(dor_num / 10000).
COMPUTE dor_m = TRUNC((dor_num - dor_y * 10000) / 100).
COMPUTE dor_d = dor_num - dor_y * 10000 - dor_m * 100.

COMPUTE dod_y = TRUNC(dod_num / 10000).
COMPUTE dod_m = TRUNC((dod_num - dod_y * 10000) / 100).
COMPUTE dod_d = dod_num - dod_y * 10000 - dod_m * 100.
EXECUTE.

* Build standard Dates.
COMPUTE date_reg = DATE.MDY(dor_m, dor_d, dor_y).
COMPUTE date_death = DATE.MDY(dod_m, dod_d, dod_y).
FORMATS date_reg date_death (ADATE10).

* Compute delay (in days).
COMPUTE delay_days = (date_death - date_reg) / 86400.
EXECUTE.

* DEBUG: Check if the math worked or produced missings (.).
LIST VARIABLES=dor date_reg date_death delay_days /CASES=10.

* ----------------------------------------------------------------
* STEP 4: FILTERING
* ----------------------------------------------------------------.
TITLE "--- DEBUG: FILTERING ---".

* Filter negative delays.
SELECT IF delay_days >= 0.
EXECUTE.

* DEBUG: Did we accidentally kill all the data?
DESCRIPTIVES VARIABLES=delay_days.

* ----------------------------------------------------------------
* STEP 5: JOINING
* ----------------------------------------------------------------.
TITLE "--- DEBUG: JOINING REGIONS ---".

SORT CASES BY region_code.

MATCH FILES
  /FILE=*
  /TABLE='temp_regions.sav'
  /BY region_code.
EXECUTE.

* DEBUG: Check if region_name is populated (or empty if join failed).
FREQUENCIES VARIABLES=region_name.

* ----------------------------------------------------------------
* STEP 6: AGGREGATION
* ----------------------------------------------------------------.
TITLE "--- DEBUG: AGGREGATION ---".

* Categorise income.
STRING income_band (A10).
IF (income < 20000) income_band = 'LOW'.
IF (income >= 20000 AND income < 40000) income_band = 'MEDIUM'.
IF (income >= 40000) income_band = 'HIGH'.
EXECUTE.

* Aggregate.
AGGREGATE
  /OUTFILE=*
  /BREAK=region_name income_band
  /TOTAL_DEATHS = N
  /AVG_DELAY = MEAN(delay_days).
EXECUTE.

* DEBUG: This is what we are about to save.
LIST VARIABLES=ALL.

* ----------------------------------------------------------------
* STEP 7: SAVE
* ----------------------------------------------------------------.
TITLE "--- DEBUG: SAVING ---".

SAVE TRANSLATE
  /OUTFILE='final_output.csv'
  /TYPE=CSV
  /MAP
  /REPLACE.