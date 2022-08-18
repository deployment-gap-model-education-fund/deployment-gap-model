# existing_plants

This table describes existing power plants from the PUDL database (mostly EIA data). Each row represents one plant.

## Modeling Decisions

### Multiple Fuel Types

Many power plants have multiple generators and about 11% of plants have generators that run on different fuels (such as a co-located coal plant and natural gas turbine). To simplify this table, plants are classified by only one fuel type – the fuel type responsible for the largest share of electrical generation. Where generation data is not available, the fuel type with the largest capacity is used.
