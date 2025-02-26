-- This query will calculate the real-time license allocation for the given month at the point of query execution. This must be run from the master account in the org

FROM NrMTDConsumption --Consumption data from the NrMTDConsumption event type which will give us the last known billable users count for the current month
    LEFT JOIN ( 

        SELECT
               sum(
                   if ((`changedFrom` = 'Basic' OR `changedFrom` = 'Core' OR `changedFrom` IS NULL) AND (`changedTo` = 'Full' OR`changedTo` = 'Basic' OR`changedTo` IS NULL) OR `userDeleted` = 1 OR`userCreatedWithFull` = 1,
                    if (( `changedFrom` = 'Basic'  OR `changedFrom` = 'Core'  OR (`changedFrom` IS NULL AND `userCreatedWithFull` = 1)) AND ( `changedTo` = 'Full'  OR (`changedTo` IS NULL AND `userCreatedWithFull` = 1)),1,0),-1)
                  ) as FSOCount, 

            sum(userUpgradeFromBasicToFull) as 'userUpgradeFromBasicToFull',
            sum(userUpgradeFromCoreToFull) as 'userUpgradeFromCoreToFull',
            sum(userDowngradeFromFulltoBasic) as 'userDowngradeFromFulltoBasic',
            sum(userDowngradeFromFulltoCore) as 'userDowngradeFromFulltoCore',
            sum(userDowngradeFromCoretoBasic) as 'userDowngradeFromCoretoBasic',
            sum(userCreatedWithFull) as 'userCreatedWithFull',
            sum(userCreatedWithCore) as 'userCreatedWithCore',
            sum(userCreatedWithBasic) as 'userCreatedWithBasic',
            sum(userDeleted) as 'userDeleted'
             
        FROM -- Subquery to allow sum() aggregation above
            ( 
FROM NrAuditEvent -- Query the NrAuditEvent event type for audit data
                WITH 
                    //Parse the three different types of userType impacting events 
                    aparse(description, 'User * type was changed from * to *.') as (`changedEmail`, `changedFrom`, `changedTo`), 
                    aparse(description, 'User * was created with user type *.') as (`createdEmail`, `createdWith`),
                    aparse(description, 'User * was deleted.') as (`deletedEmail`),
                    
                    //Consolidate the common variables into a single attribute
                    if(changedEmail is not null or createdEmail is not null, if(changedEmail is not null, changedEmail, createdEmail), deletedEmail) as `email`,
                    if(changedTo is null, createdWith, changedTo) as `userType`,

                    //Actin logic per userType impacting events
                    if(`changedFrom` = 'Basic' and `changedTo` = 'Full', 1,0) as userUpgradeFromBasicToFull,
                    if(`changedFrom` = 'Core' and `changedTo` = 'Full', 1,0) as userUpgradeFromCoreToFull,
                    if(`changedFrom` = 'Full' and `changedTo` = 'Basic', 1,0) as userDowngradeFromFulltoBasic,
                    if(`changedFrom` = 'Full' and `changedTo` = 'Core', 1,0) as userDowngradeFromFulltoCore,
                    if(`changedFrom` = 'Core' and `changedTo` = 'Basic', 1,0) as userDowngradeFromCoretoBasic,
                    if(`createdWith` = 'Full', 1,0) as userCreatedWithFull,
                    if(`createdWith` = 'Core', 1,0) as userCreatedWithCore,
                    if(`createdWith` = 'Basic', 1,0) as userCreatedWithBasic,
                    if(description LIKE '%was deleted.', 1,0) as userDeleted

                SELECT 
                    earliest(`changedFrom`) as 'changedFrom',
                    latest(`changedTo`) as 'changedTo',
                    filter(count(`userUpgradeFromBasicToFull`), WHERE userUpgradeFromBasicToFull = 1) as 'userUpgradeFromBasicToFull',
                    filter(count(`userUpgradeFromCoreToFull`), WHERE userUpgradeFromCoreToFull = 1) as 'userUpgradeFromCoreToFull',
                    filter(count(`userDowngradeFromFulltoBasic`), WHERE userDowngradeFromFulltoBasic = 1) as 'userDowngradeFromFulltoBasic',
                    filter(count(`userDowngradeFromFulltoCore`), WHERE userDowngradeFromFulltoCore = 1) as 'userDowngradeFromFulltoCore',
                    filter(count(`userDowngradeFromCoretoBasic`), WHERE userDowngradeFromCoretoBasic = 1) as 'userDowngradeFromCoretoBasic',
                    filter(count(`userCreatedWithFull`), WHERE userCreatedWithFull = 1) as 'userCreatedWithFull',
                    filter(count(`userCreatedWithCore`), WHERE userCreatedWithCore = 1) as 'userCreatedWithCore',
                    filter(count(`userCreatedWithBasic`), WHERE userCreatedWithBasic = 1) as 'userCreatedWithBasic',
                    filter(count(`userDeleted`), WHERE userDeleted = 1) as 'userDeleted',
                    latest(accountId()) as accountId

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
    latest(userUpgradeFromBasicToFull) as 'Number of Basic to Full Actions',
    latest(userUpgradeFromCoreToFull) as 'Number of Core to Full Actions',
    latest(userDowngradeFromFulltoBasic) as 'Number of Full to Basic Actions',
    latest(userDowngradeFromFulltoCore) as 'Number of Full to Core Actions',
    latest(userDowngradeFromCoretoBasic) as 'Number of Core to Basic Actions',
    latest(userCreatedWithFull) as 'Users created with Full',
    latest(userCreatedWithCore) as 'Users created with Core',
    latest(userCreatedWithBasic) as 'Users created with Basic',
    latest(userDeleted) as 'No. of Deleted Users (this will not reflect in "FSO Count as of Now"'


-- For the outter NrMTDConsumption query, limit the time window to last month because we only want the latest(FullPlatformUsersBillable)
SINCE this month 