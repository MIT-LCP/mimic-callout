# callout [![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.1410655.svg)](https://doi.org/10.5281/zenodo.1410655)

The research question of this study is: Do discharge delays from the ICU adversely affect patient outcomes?

This research was published in the Journal of Intensive Care Medicine as: [Impact of Intensive Care Unit Discharge Delays on Patient Outcomes: A Retrospective Cohort Study](https://doi.org/10.1177/0885066618800276)

```
@article{bose2018impact,
  title={Impact of Intensive Care Unit Discharge Delays on Patient Outcomes: A Retrospective Cohort Study},
  author={Bose, Somnath and Johnson, Alistair EW and Moskowitz, Ari and Celi, Leo Anthony and Raffa, Jesse D},
  journal={Journal of intensive care medicine},
  pages={0885066618800276},
  year={2018},
  publisher={SAGE Publications Sage CA: Los Angeles, CA},
  doi={10.1177/0885066618800276}
}
```

This research was generously supported by grants 1U01HL123022-01 (NIH/NHLBI), R01-
EB017205 (NIH/NIBIB), R01-GM104987 (NIH/NIBIB), and 2T32HL007374-37 (NIH).

# Data extraction

Data extraction is done in a set of SQL scripts numbered according to the order in which they should be executed (0-4). Note that data extraction is done in two distinct systems: (i) month/year PHI data (along with hospital census information) is extracted in 0-phi-info.sql, (ii) covariates available in MIMIC-III are extracted in SQL scripts 1-4. Researchers in the LCP at MIT will be able to run script 0-phi-info.sql and extract a file with additional PHI info. All other researchers should ignore this script, and run scripts 1-3, to generate the callout data.

Once scripts 1-3 have been executed, the data can be copied to file. To generate all the data, run the following in PostgreSQL:

```sql
\cd /home/alistairewj/callout/
\i 1-define-cohort.sql
\i 2-icu-variables.sql
\i 3-final-design-matrix.sql
copy (select * from dd_design_matrix order by icustay_id) to '/home/alistairewj/callout/callout.csv' CSV HEADER;
```
