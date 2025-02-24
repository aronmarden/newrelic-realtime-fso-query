-- This query will calculate the real-time license allocation for the given month at the point of query execution. This must be run from the master account in the org

FROM NrMTDConsumption --Consumption data from the NrMTDConsumption event type which will give us the last known billable users count for the current month
    LEFT JOIN ( 
        SELECT
            sum(
                 if((`latestChange` = 'Full' or `latestChange` = 'Basic') and (`earlistFrom` = 'Basic' or `earlistFrom` = 'Core' or `earlistFrom` is null), 
                     if(`latestChange` = 'Full' and (`earlistFrom` = 'Basic' or `earlistFrom` = 'Core' or `earlistFrom` is null), 1, 0), -1) as FSOCount

               ) as FSOCount, --license allocation logic; add 1 if latest change is 'Full' and earliest type was 'Basic' or 'Core'; subtract 1 otherwise
            sum(userUpgradeFlag) as 'upgradeCount',
            sum(userUpgradeFlagFull) as 'fullUpgradeCount',
            sum(userUpgradeFlagCore) as 'coreUpgradeCount',
            sum(userDowngradeFlag) as 'fullDowngradeCount',
            sum(userCreatedFlag) as 'createdCount',
            sum(userDeletedFlag) as 'deletedCount'
             
        FROM -- Subquery to allow sum() aggregation above
            ( 
                FROM NrAuditEvent -- Query the NrAuditEvent event type for audit data
                WITH 
                    aparse(description, 'User * type was changed from * to *.') as (`email1`, `from`, `to1`), -- Parse 'description' to extract user email, previous userType, and new userType
                    aparse(description, 'User * was created with user type *.') as (`email2`, `to2`), -- Parse 'description' to extract user email, previous userType, and new userType
                    aparse(description, 'User * was *.') as (`email3`, `action`), 
                    if(description LIKE 'User % was%', if(description LIKE 'User % was created%', email2, email1), email3) as `email`,
                    if(to1 is null, to2, to1) as `to`,

                    if(description LIKE 'User % type was changed from Basic to Full.', 1,0) as userUpgradeFlagFull,
                    if(description LIKE 'User % type was changed from Core to Full.', 1,0) as userUpgradeFlagCore,
                    if(userUpgradeFlagFull = 1 or userUpgradeFlagCore = 1, 1, 0) as userUpgradeFlag,
                    if(description LIKE 'User % type was changed from Full to Basic.', 1, 0) as userDowngradeFlagBasic,
                    if(description LIKE 'User % was created with user type Full.', 1,0) as userCreatedFlag,
                    if(description LIKE '%was deleted.', 1,0) as userDeletedFlag

                SELECT
                    latest(`to`) as latestChange, -- Capture the most recent new type (i.e., latest change)
                    earliest(`from`) as earlistFrom, -- Capture the original type before any changes
                    latest(userUpgradeFlag) as userUpgradeFlag, 
                    latest(userDowngradeFlagBasic) as userDowngradeFlag,
                    latest(userCreatedFlag) as userCreatedFlag,
                    latest(userDeletedFlag) as userDeletedFlag,
                    latest(accountId()) as accountId -- Retrieve the associated account ID for the JOIN (since this data is only available in the NrAuditEvent from the master account, this join value will always match the masterAccountId in the NrMTDConsumption event
                WHERE
                    description LIKE '% type was changed from %' or description LIKE '%created with user type%' or description LIKE '%was deleted.'-- Filter for events that involve a userType change for both self tier upgrade and admin performed ugprade
                WHERE
                    `email` NOT LIKE '%@newrelic.com%'  -- Exclude internal New Relic email events
                FACET `email` -- Group the results by user email to track events that have multiple userType changes for the same user
                LIMIT MAX -- Ensure all possible results are presented to the outter-nested query.
            )         
        SINCE this month  -- Limit this subquery to events occurring in the current month only, as they are the delta's from the latest(NrMTDConsumption) full user count
        FACET accountId -- Group the subquery results by accountID() and masterAccountId in the outter query
    ) ON string(masterAccountId) = accountId 

WITH 

string(masterAccountId) as masterAccountIdString, -- masterAccountId needs to be stringafied since FACETs values (which is what accountId is now) are always strings
if(FSOCount is null, 0, FSOCount) as FSOCountChecked

SELECT 

earliest(FullPlatformUsersBillable) as 'Start of Month FSO Count',
(latest(FSOCountChecked) + earliest(FullPlatformUsersBillable)) as 'FSO Count as of Now', -- Take the current license allocation movements from NrAuditEvent for this month and sum them to the last known full monthly billable users count from NrMTDConsumption
latest(upgradeCount) as 'No. of Upgrades',
latest(fullDowngradeCount) as 'No. of Downgrades',
latest(createdCount) as 'No. of User Creations (Full)',
latest(deletedCount) as 'No. of Deleted Users (this will not reflect in "FSO Count as of Now"'


-- For the outter NrMTDConsumption query, limit the time window to last month because we only want the latest(FullPlatformUsersBillable)
SINCE this month 
