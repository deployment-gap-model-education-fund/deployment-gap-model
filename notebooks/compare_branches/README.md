# Compare Branches

This folder contains notebooks to help with comparing the data outputs between two branches in the repo, in order to perform sanity checks on any changes in the code. This is not for regular data updates, which are collected in the `data_updates` folder.

## Manual Instructions

The manual way to compare the data outputs is to create a subfolder `data/tmp/<your-new-branch-name>` as well as `data/tmp/<base-branch-name>` (most likely, the base branch will be `dev`).

Then with the feature branch checked out, run `make all` and move the generated parquet files you'd like to compare from `data/output/...` into the feature branch folder under `data/tmp`.
Similarly, with the base branch checked out, run `make all` again and move those files under the base branch folder under `data/tmp`.

Now create a notebook that reads those parquet files and perform sanity checks.
