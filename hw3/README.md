This folder contains scripts to solve homework for week 3 of the Data Engineering Zoomcamp for the 2025 cohort. For the actual questions, look [here](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/cohorts/2025/03-data-warehouse/homework.md)

Before running the homework, make sure to update the virtualenv by running (in top-level folder)

```
source .dezoomcamp/bin/activate
uv pip install -r setup/requirements.txt
```

You will need to set the following environmental variables:

- CREDENTIALS_FILE: this should be the path to the `json` file with your GCP service account credentials
- BUCKET: the name of the bucket to use for this homework. Must be unique!
- PROJECT: the name of your GCP project

Finally you will need to upload the data to the GCP bucket. For that, execute

```bash
python load_yellow_tax_data.py
```

When all ready, simply run

```bash
python hw3.py
```