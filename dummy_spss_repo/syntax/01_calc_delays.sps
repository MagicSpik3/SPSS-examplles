* PURPOSE: Load data and calculate delays.

GET DATA /TYPE=TXT
  /FILE='../input_data.csv'
  /ARRANGEMENT=DELIMITED
  /DELCASE=LINE
  /FIRSTCASE=2
  /DELIMITERS=","
  /VARIABLES=
  dor A8
  dod A8.

* Convert and Calculate (The logic we verified earlier).
COMPUTE dor_num = NUMBER(dor, F8.0).
COMPUTE dod_num = NUMBER(dod, F8.0).
COMPUTE dor_y = TRUNC(dor_num / 10000).
COMPUTE dor_m = TRUNC(MOD(dor_num, 10000) / 100).
COMPUTE dor_d = MOD(dor_num, 100).
COMPUTE dod_y = TRUNC(dod_num / 10000).
COMPUTE dod_m = TRUNC(MOD(dod_num, 10000) / 100).
COMPUTE dod_d = MOD(dod_num, 100).
COMPUTE date_reg = DATE.MDY(dor_m, dor_d, dor_y).
COMPUTE date_death = DATE.MDY(dod_m, dod_d, dod_y).
FORMATS date_reg date_death (DATE11).

COMPUTE delay_seconds = date_reg - date_death.
COMPUTE delay_days = delay_seconds / 86400.

SELECT IF (delay_days >= 0).
EXECUTE.
