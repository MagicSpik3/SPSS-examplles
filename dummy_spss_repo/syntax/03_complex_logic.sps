* 03_complex_logic.sps.
* Intent: Categorize age and summarize delays by region.

DATA LIST FREE / id (F8.0) region (A10) age (F3.0) delay_days (F5.0).
BEGIN DATA.
101 "North" 25 4
102 "South" 65 10
103 "North" 45 2
104 "East"  80 15
105 "South" 30 0
END DATA.

* 1. Complex Recode using 'case when' logic.
STRING age_group (A10).
RECODE age (Lowest thru 18 = "Minor")
           (19 thru 64 = "Adult")
           (65 thru Highest = "Senior")
           INTO age_group.
EXECUTE.

* 2. Filter logic.
SELECT IF (delay_days >= 0).

* 3. Aggregation (The hard part).
* SPSS often saves this to a separate dataset, but in R we want a dataframe.
AGGREGATE
  /OUTFILE=* MODE=ADDVARIABLES
  /BREAK=region
  /mean_delay = MEAN(delay_days)
  /max_age = MAX(age).
