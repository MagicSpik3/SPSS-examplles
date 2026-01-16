* Hello World Pipeline.

* 1. LOAD (Now skips the header row!).
GET DATA
  /TYPE=TXT
  /FILE='hello.csv'
  /ARRANGEMENT=DELIMITED
  /DELIMITERS=','
  /FIRSTCASE=2
  /VARIABLES=
    id F8.0
    name A10.

* 2. SORT (The Test Case).
SORT CASES BY id.

* 3. SAVE (Using TRANSLATE to get a readable CSV).
SAVE TRANSLATE
  /OUTFILE='hello_sorted.csv'
  /TYPE=CSV
  /REPLACE.