# New Relic Real-Time FSO Query

This SQL query calculates the real-time license allocation for the given month at the point of query execution. It must be run from the master account in the organization.

## Query Overview

The query performs the following operations:

1. **Fetch Consumption Data**: Retrieves consumption data from the `NrMTDConsumption` event type, which provides the last known billable users count for the current month.
2. **Join with Audit Data**: Joins the consumption data with audit data from the `NrAuditEvent` event type to calculate the license allocation movements for the current month.
3. **Calculate License Allocation**: Uses various flags and conditions to determine the number of upgrades, downgrades, user creations, and deletions.
4. **Generate Final Results**: Aggregates the data to provide the start of month FSO count, current FSO count, number of upgrades, downgrades, user creations, and deletions.

## Query Details

### Consumption Data

The query starts by fetching data from the `NrMTDConsumption` event type:

```sql
FROM NrMTDConsumption
```

### Join with Audit Data

The query joins the consumption data with audit data from the `NrAuditEvent` event type:

```sql
LEFT JOIN (
    SELECT
        ...
    FROM (
        FROM NrAuditEvent
        ...
    )
    ...
) ON string(masterAccountId) = accountId
```

### License Allocation Logic

The query calculates the license allocation using various flags and conditions:

```sql
sum(
    if((`latestChange` = 'Full' or `latestChange` = 'Basic') and (`earlistFrom` = 'Basic' or `earlistFrom` = 'Core' or `earlistFrom` is null), 
        if(`latestChange` = 'Full' and (`earlistFrom` = 'Basic' or `earlistFrom` = 'Core' or `earlistFrom` is null), 1, 0), -1) as FSOCount
) as FSOCount
```

### Final Results

The query generates the final results, including the start of month FSO count, current FSO count, number of upgrades, downgrades, user creations, and deletions:

```sql
SELECT 
    earliest(FullPlatformUsersBillable) as 'Start of Month FSO Count',
    (latest(FSOCountChecked) + earliest(FullPlatformUsersBillable)) as 'FSO Count as of Now',
    latest(upgradeCount) as 'No. of Upgrades',
    latest(fullDowngradeCount) as 'No. of Downgrades',
    latest(createdCount) as 'No. of User Creations (Full)',
    latest(deletedCount) as 'No. of Deleted Users (this will not reflect in "FSO Count as of Now"'
```

## Notes

- The query must be run from the master account in the organization.
- The time window for the outer `NrMTDConsumption` query is limited to the current month to ensure accurate results for the start of the month billing, in order to carry over the changes from the previous month. 