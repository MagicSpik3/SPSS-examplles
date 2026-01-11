* PURPOSE: Summarize deaths by month.
* DEPENDENCY: Requires 'date_death' from previous step.

COMPUTE death_month = XDATE.MONTH(date_death).
FORMATS death_month (F2.0).

AGGREGATE
  /OUTFILE=* MODE=ADDVARIABLES
  /BREAK=death_month
  /total_deaths=N.

EXECUTE.

* Output for verification.
LIST variables=death_month total_deaths /CASES=FROM 1 TO 10.
