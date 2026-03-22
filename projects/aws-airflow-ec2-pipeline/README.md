# CLE Departures Pipeline

End-to-end data engineering pipeline that ingests flight departure data from the AviationStack API, orchestrated with Dagster and processed in Databricks using a medallion architecture with a built-in dashboard. The pipeline runs on a daily schedule and is designed to handle limitations of the free API tier, which restricts responses to 100 flights per request. Future enhancements include expanding coverage to continental U.S. routes and developing a standby travel model to identify routes with the highest number of flight options.
<br><br>


<img width="613" height="268" alt="Screenshot 2026-03-21 125658" src="https://github.com/user-attachments/assets/e06fdcca-f196-48e2-8205-e373742cbfb2" />
<br><br>

<img width="1914" height="385" alt="Screenshot 2026-03-20 220952" src="https://github.com/user-attachments/assets/36d0ded3-88bd-44e5-b5bb-02dd6e817d29" />
<br><br>

<img width="1065" height="659" alt="Screenshot 2026-03-21 122735" src="https://github.com/user-attachments/assets/df4ffecb-e05f-44d2-a345-2353e2023b39" />
