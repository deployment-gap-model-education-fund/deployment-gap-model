# ncsl__state_permitting

This table documents the last modification date of all input files to the Deployment Gap Model database.

## Table Details

**Source:** All input data

**Grain:** One row per dataset

**Primary key column(s):** `dataset_name`

**Purpose:** Identify the source path and last file modification date of each data source used in the pipeline.

## Transformations

The ETL checks each path and captures the most recent file modification date.
For directories, this is the most recent file modification date of any file in the folder.
For files stored on GitHub, this is the last modification date on the GitHub repository.

Note that this reflects the last date that the file was updated within the data
pipeline, rather than the original date of file creation - i.e., if a file was created
on June 1st and added to the ETL on June 5th, it will appear here with a modification
date of June 5th. This will be the case for sources that are stored in the data folder of the Github repository,
or manually added to the dgm-archive GCS bucket. For datasets that are pulled directly from the source (e.g., PUDL),
the date of creation and modification will be identical.
