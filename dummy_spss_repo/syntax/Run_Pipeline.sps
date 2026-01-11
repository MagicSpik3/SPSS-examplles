* MASTER CONTROLLER.

* 1. Run ETL.
INSERT FILE='01_calc_delays.sps'.

* 2. Run Analysis.
INSERT FILE='02_summarize_deaths.sps'.

* 3. Run Complex Recodes (New Stage).
INSERT FILE='03_complex_logic.sps'.

* 4. Finish.
ECHO "Pipeline Complete".
