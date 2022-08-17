# CO2 Estimation

Comparing power plants to petrochemical facilities requires a common metric. EIP already provides CO2e estimates (based on 100-year Global Warming Potential) for their infrastructure facilities, so we created companion estimates for fossil power plants. These figures cover direct combustion emissions only. Methane leakage, for example, is out of scope.

## De-rate EIP's CO2e Estimates by 15%

EIP's CO2e emissions estimates come from Clean Air Act permitting documents. These estimates are defined as the emissions a facility would produce at 100% utilization. Such high utilization is rarely acheived in practice, so we de-rate the estimates accordingly. National average capacity utilization for refineries is about 85% ([per EIA](https://www.eia.gov/dnav/pet/pet_pnp_unc_dcu_nus_m.htm)), and for the chemical industry as a whole is about 75% ([per FRED](https://fred.stlouisfed.org/series/CAPUTLG325S)). We apply a coarse adjustment of -15% to CO2e estimates from EIP.

There are several possible sources of bias in these estimates:

* new facilities may be more economically competitive (and thus run at higher utilization) than the older facilities in sector-wide aggregates
* the original sources of estimation are the owners of the facilities who produce the permitting reports. They may produce biased estimates
* pollution controls may not be operated to their fullest capacity in the absence of adequate enforcement

## Estimating Power Plant Emissions

Our approach to estimating CO2e is to first estimate the primary fuel consumption at each power plant then multiply that by the EPA’s fuel emissions factors (CO2e per unit) to yield CO2e.

### Existing Power Plants

Existing power plants report input fuel consumption to the EIA, so this is straightforward. But proposed plants from the ISO queues are more complicated.

### Proposed Power Plants

Proposed plants lack information about hypothetical fuel consumption, so we have to estimate that first. The ISO queues do not give us much information to work with – we really only know the capacity and fuel type. We use data about recent existing plants to estimate these values for proposed plants.

The chain of estimation is as follows:

1. Classify the plant technology based on capacity and fuel type (combustion turbine or combined cycle)
1. Estimate capacity factor based on recent (2015+) existing plants of similar capacity and technology type.
1. Estimate electrical production by multiplying reported capacity with this estimated capacity factor
1. Estimate input fuel consumption by combining electrical production estimates with technology-specific combustion efficiency [(“heat rates”) from the EIA](https://www.eia.gov/electricity/annual/html/epa_08_02.html).

#### Uncertainty

Each step of this process introduces error, but thankfully the largest emitters happen to be the most common and the most certain.
The largest source of relative error is infrequent. Erroneously confusing a “peaker” combustion turbine with a combined cycle plant will result in a whopping 3.9x difference in estimated CO2e (due to large differences in capacity factor). 24/242 (10%) of 2020 ISO gas plants are in an ambiguous capacity range (100 - 180 MW) where technology type is not clear. BUT only 4% of total capacity is in that range.
Small gas plants (<40 MW) have huge range in capacity factors, but they are small so that uncertainty has less impact. The 2020 ISO queues have 78/242 (32%) small gas plants by count, but only 1/65 GW (1.5%) by capacity.
Large plants (>180 MW) are more numerous (88/242, 36%), more important (57/65 GW, 87% of capacity), and more certain (can only be combined cycle, narrower range of capacity factor) than smaller ones.
