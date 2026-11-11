# Overview
There are two tables which provide useful metadata for understanding the data in a
deployment. These tables contain information about when input data was updated, and
when published tables are updated.

|        Table        |       Description        |
| ------------------- | ------------------------ |
| `madrone__data_last_updated` | Indicates when each input data source was last updated. |
| `madrone__deployment_metadata` | Indicates when a published table was last modified. Published tables change when inputs change or when data transform code changes. |
