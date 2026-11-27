# Historical data sources for the RenewFlow benchmark

The four-authority benchmark needs actual hourly solar and wind generation, actual authority-wide demand, and at least one complete load zone per authority. It starts January 15, 2025 UTC and uses the same frozen inputs for both runners. EIA-930 bulk files offer the closest historical fit. OPSD can support a separate European replay, and AEMO is a candidate for a separate higher-volume workload. [Benchmark protocol](BENCHMARKS.md)

## EIA-930 via Catalyst Cooperative's PUDL

Catalyst republishes Form EIA-930 submissions from balancing authorities in the Lower 48 states, including CISO. EIA added hourly generation by fuel and demand for selected load zones in July 2018. The underlying sources are metered net generation and calculated demand based on generation and interchange. Its generation coverage is limited to what balancing authorities can see, and typically excludes rooftop PV. EIA publishes six-month Balance and Subregion CSV files through the Hourly Electric Grid Monitor. [EIA-930 data and downloads](https://www.eia.gov/electricity/930-content/about.html), [PUDL source documentation](https://docs.catalyst.coop/pudl/en/stable/data_sources/eia930.html)

The benchmark reads the four supplied 2025 Balance and Subregion CSVs in the timed Extract tasks. Its manifest records their SHA-256 hashes before timing. CISO authority-wide values matched the earlier frozen observations across the 30-day overlap. The reported generation fields for solar and wind are split by integrated battery storage in the supplied files. The selected observations had no nonzero contribution in either integrated-storage column. [Current benchmark input contract](BENCHMARKS.md) · [EIA reporting conventions](https://www.eia.gov/electricity/930-content/about.html)

Catalyst also publishes processed tables for net generation, operations, and subregional demand. Their reported MWh fields and UTC hour endings provide a potential alternative source for the same geography. [PUDL data dictionary](https://docs.catalyst.coop/pudl/en/stable/data_dictionaries/pudl_db.html#core-eia930-hourly-net-generation-by-energy-source) · [PUDL storage access](https://docs.catalyst.coop/pudl/en/stable/data_access.html#cloud-storage)

## Open Power System Data

The 2020-10-06 release republishes ENTSO-E Transparency data for 32 European countries, with observations organized by country, control area, or bidding zone. The package describes coverage from 2015 into 2020, with varying coverage by field and geography. The pinned hourly CSV is 130,339,665 bytes, and its `utc_timestamp` already denotes the start of the hour. Germany has `DE_load_actual_entsoe_transparency`, `DE_solar_generation_actual`, and `DE_wind_generation_actual`, all in MW. The reported wind components can differ by area. For example, the hourly schema has an onshore series for Amprion and both onshore and offshore series for TenneT Germany. When an MW value represents average power across the hour, multiply by one hour to obtain MWh. [Hourly CSV](https://data.open-power-system-data.org/time_series/2020-10-06/time_series_60min_singleindex.csv), [Release and field documentation](https://data.open-power-system-data.org/time_series/2020-10-06/), [Versioned metadata and checksum](https://data.open-power-system-data.org/time_series/2020-10-06/datapackage.json)

The file can drive a fixed historical replay with a comparatively simple column mapping. Its area IDs and dates would change the benchmark population, and the selected solar, wind, and load fields still need a complete-day coverage check. ENTSO-E operates the upstream live API and requires a separate security token. [Release documentation](https://data.open-power-system-data.org/time_series/2020-10-06/) · [ENTSO-E API access](https://transparencyplatform.zendesk.com/hc/en-us/articles/12845911031188-How-to-get-security-token)

## AEMO NEMWeb

The January 2024 MMSDM directory exposes table-specific CSV ZIPs for Australia's National Electricity Market. The `DISPATCH_UNIT_SCADA` ZIP is 22,576,818 bytes and the `DISPATCHREGIONSUM` ZIP is 4,849,914 bytes. `DISPATCH_UNIT_SCADA` contains instantaneous output in MW at five-minute intervals for scheduled, semi-scheduled, and other units where telemetry is available. Regional solar and wind totals require joining dispatchable unit IDs to effective-dated region metadata and fuel classifications. AEMO uses market time UTC+10. A five-minute MW sample times 5/60 hours provides an energy estimate, so hourly aggregation needs a documented interval convention and a check for missing samples. [January 2024 ZIP index](https://nemweb.com.au/Data_Archive/Wholesale_Electricity/MMSDM/2024/MMSDM_2024_01/MMSDM_Historical_Data_SQLLoader/DATA/), [SCADA field definitions](https://visualisations.aemo.com.au/aemo/nemweb/MMSDataModelReport/Electricity/MMS%20Data%20Model%20Report_files/MMS_123.htm), [Unit metadata](https://visualisations.aemo.com.au/aemo/nemweb/MMSDataModelReport/Electricity/MMS%20Data%20Model%20Report_files/MMS_248.htm), [AEMO registration](https://www.aemo.com.au/energy-systems/electricity/national-electricity-market-nem/participate-in-the-market/registration), [Market time specification](https://www.aemo.com.au/-/media/files/initiatives/der/2023/project-edge---data-specification-part-a.pdf)

For observed load, AEMO publishes half-hourly measured `OPERATIONAL_DEMAND` in MW through separate files. Its five-minute operational demand series is interpolated, and the January MMSDM `DISPATCHREGIONSUM.TOTALDEMAND` is a dispatch calculation that incorporates forecast and dispatch-error adjustments. Operational demand excludes demand met by sources such as rooftop PV, which AEMO publishes separately as an estimate. This combination could exercise additional extraction and aggregation at scale once the matching demand and metadata coverage is verified. The January directory itself is a historical file archive. [Operational demand and file retention](https://www.aemo.com.au/energy-systems/electricity/national-electricity-market-nem/data-nem/operational-demand-data), [Dispatch demand definitions](https://www.aemo.com.au/-/media/files/electricity/nem/security_and_reliability/dispatch/policy_and_process/demand-terms-in-emms-data-model.pdf), [Rooftop PV methodology](https://www.aemo.com.au/energy-systems/electricity/national-electricity-market-nem/nem-forecasting-and-planning/operational-forecasting/solar-and-wind-energy-forecasting/australian-solar-energy-forecasting-system)

## Live API option

Open Electricity offers an Australian API with regional generation grouped by fuel and demand as a market metric. This is a simpler live option than joining raw MMSDM tables. It uses an API key and applies request-window and historical-access limits. No comparative uptime measurements were collected. [Generation endpoint](https://docs.openelectricity.org.au/api-reference/data/get-network-data/) · [Demand endpoint](https://docs.openelectricity.org.au/api-reference/market/get-network-data/) · [Data limits](https://docs.openelectricity.org.au/api-reference/data-limits/)

## Downloads and verification

On November 26, 2026, shell download attempts for OPSD, NEMWeb, EIA, and Zenodo failed at DNS resolution in the workspace. The web browser confirmed the publisher listings and the Balance and Subregion members in the first and last Zenodo ZIPs. Those download attempts saved no files. The local import was verified later with the four supplied CSVs. [OPSD hourly file](https://data.open-power-system-data.org/time_series/2020-10-06/time_series_60min_singleindex.csv) · [NEMWeb January sample demand CSV](https://www.nemweb.com.au/Reports/CURRENT/WebData/NEM/PRICEANDDEMAND/PRICE_AND_DEMAND_202401_NSW1.CSV) · [First EIA-930 archive contents](https://zenodo.org/records/22525116/preview/eia930-2025half1.zip) · [Last EIA-930 archive contents](https://zenodo.org/records/22525116/preview/eia930-2026half1.zip)

For a separate research run that extends into January 2026, the following example downloads three archive periods. The archive record lists expected MD5 checksums. Keep the ZIPs and the calculated checksums with the input provenance. The commands extract the Balance and Subregion CSVs into a workspace-local directory ignored by Git. [Archive download page and checksums](https://zenodo.org/records/22525116)

```sh
(
  set -e
  mkdir -p .renewflow/source-samples/eia930
  cd .renewflow/source-samples/eia930
  for period in 2025half1 2025half2 2026half1; do
    name="eia930-${period}.zip"
    curl -fL --retry 2 "https://zenodo.org/records/22525116/files/${name}?download=1" -o "$name"
    unzip -tq "$name"
    unzip -n "$name" '*-balance.csv' '*-subregion.csv'
  done
  shasum -a 256 ./*.zip ./*.csv
)
```

### Zenodo CLI check

The requested record `24438986` was unreachable from the workspace. Running `zenodo_get` via `uvx` with workspace-accessible tool and cache directories failed at PyPI DNS resolution. A direct request for the record metadata also failed at Zenodo DNS resolution. The download directory `.renewflow/source-samples/zenodo-24438986` contains no archives, so that record’s identity, ZIP integrity, and CSV contents remain unverified. [Requested Zenodo record](https://zenodo.org/records/24438986) · [Zenodo CLI documentation](https://github.com/dvolgyes/zenodo_get#usage)
