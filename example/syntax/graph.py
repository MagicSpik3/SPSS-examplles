digraph SPSS_Pipeline {

  rankdir=LR;
  node [shape=box];

  S0 [label="Start"];

  S1 [label="Control Raw"];
  S2 [label="Control Parsed"];
  S3 [label="Control Single Row"];

  S4 [label="Rates Raw"];
  S5 [label="Rates Sorted"];

  S6 [label="Claims Raw"];
  S7 [label="Claims + join_key"];
  S8 [label="Claims + Controls"];
  S9 [label="Claims + Dates"];
  S10 [label="Claims + Age/Status"];
  S11 [label="Claims + Month Window"];
  S12 [label="Claims + Rates + Payment"];
  S13 [label="Filtered Claims"];
  S14 [label="Aggregated Summary"];
  S15 [label="CSV Output", shape=oval];

  S0 -> S1 -> S2 -> S3;
  S0 -> S4 -> S5;
  S0 -> S6 -> S7 -> S8;
  S3 -> S8;

  S8 -> S9 -> S10 -> S11 -> S12;
  S5 -> S12;

  S12 -> S13 -> S14 -> S15;
}
