SELECT 
    'EQ' AS AssetClass,
    tm.messageid,
    tm.msgtype,
    t.PUBLISHERTRADEID,
    t.PUBLISHERTRADEVERSION,
    t.TradeIdType,
    alt.alternatepublishertradeid,
    alt.alternatetradeidtype,
    alt.alternatepublishertradeversion,
    t.ProductType,
    t.ProductSubType,
    t.book,
    tmj.JURISDICTION,
    t.globaluti,
    t.USI,
    t.GTRCreatedUSIPrefix,
    t.MatDate,
    t.ClearingStatus,
    t.LegalEntityUCI,
    t.CtyUCI,
    tm.srfeventtype AS eventname,
    tm.exentreason,
    tm.gtreventtype,
    tm.TM_ExecutionDateTime,
    t.eventdatetime,
    t.esmpid,
    eu.upi
FROM 
    trade AS t
INNER JOIN 
    alternatetrade AS alt ON t.tradeid = alt.tradeid
INNER JOIN 
    trademessage AS tm ON t.tradeid = tm.tradeid
INNER JOIN 
    trademessagerptjurisdiction AS tmj ON tm.trademessageid = tmj.trademessageid
LEFT JOIN 
    esmpuri AS eu ON eu.publishertradeid = t.PUBLISHERTRADEID
WHERE 
    tmj.jurisdiction IN ('CFTC', 'SEC') 
    OR tmj.jurisdiction LIKE 'CA.%'
    AND tm.msgtype NOT IN ('REAL_TIME')
    AND tmj.srfmsgstate = 'rep'
    AND t.assetclassid = 6
    AND CAST(tm.arrivaldatetime AS DATE) = DATE '2024-07-31'
    AND alt.alternatetradeidtype IN (
        'atlasCompositeReference', 'barxFxTsTradeId', 'charmsTradeId', 
        'delta1TradeId', 'echelonTradeId', 'emTmsTradeId', 
        'fiTmsTradeId', 'mptraderTradeId', 'openlinkDealId', 
        'qaBookDealId', 'simTradeId', 'soldTradeStoreTradeId', 
        'sophisCompositeReference', 'stgMptraderTradeId', 
        'synfinyTradeId'
    )
GROUP BY 
    tm.msgtype,
    t.PUBLISHERTRADEID,
    t.PUBLISHERTRADEVERSION,
    t.TradeIdType,
    alt.alternatepublishertradeid,
    alt.alternatetradeidtype,
    alt.alternatepublishertradeversion,
    t.ProductType,
    t.ProductSubType,
    t.book,
    tmj.JURISDICTION,
    t.globaluti,
    t.USI,
    t.GTRCreatedUSIPrefix,
    t.MatDate,
    t.ClearingStatus,
    t.LegalEntityUCI,
    t.CtyUCI,
    tm.srfeventtype,
    tm.exentreason,
    tm.gtreventtype,
    tm.TM_ExecutionDateTime,
    t.eventdatetime,
    t.esmpid,
    eu.upi,
    tm.messageid
LIMIT 10;
