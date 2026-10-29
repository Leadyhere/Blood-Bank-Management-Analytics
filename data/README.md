# Official public dataset

`raw/blood_donations_state.csv`: unchanged **Daily Blood Donations by Blood Group & State**, National Blood Centre / Ministry of Health Malaysia, via [data.gov.my](https://data.gov.my/data-catalogue/blood_donations_state).

License: [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/). Retain attribution when sharing. No endorsement implied.

`dataset_manifest.json` records source URL, hash, file size, dates and quality checks. The importer types dates/counts and adds provenance; charts are derived aggregations. See [DATASET.md](../DATASET.md) for commands, schema, coverage and subtotal handling. The active importer writes Parquet for Spark SQL, not MongoDB. These are aggregate public statistics, not donor identities or inventory units.
