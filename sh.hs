SELECT 
    iso.Tradeid, 
    iso.AssetClass, 
    cv.CobDate, 
    iso.ReportingCounterPartyLEI, 
    iso.OtherCounterpartyLEI, 
    iso.TransactionIdentificationUniqueTransactionIdentifier, 
    trpt.jurisdiction,
    iso.ReportingTimestamp, 
    cv.State,
    cv.Value, 
    cv.Currency, 
    cv.ValuationDateTime, 
    cv.Status, 
    cv.VmPortfolioCode, 
    cv.ExcessCollateralPosted,
    cv.ExcessCollateralReceived, 
    cv.InitialMarginPosted, 
    cv.InitialMarginReceived, 
    cv.VariationMarginPosted, 
    cv.VariationMarginReceived, 
    cv.Collateralized, 
    ActionType
INTO 
    #Finaldata
FROM 
    onereg_main.ISOPrimaryPayload (NOLOCK) iso
INNER JOIN 
    onereg_main.trademessagerptjurisdiction (NOLOCK) trpt ON trpt.tmjid = iso.tmjid
INNER JOIN 
    onereg_main.ISOsecondaryPayload (NOLOCK) isosec ON isosec.TradeMessageISOPayloadID = iso.ISOPrimaryPayloadID
INNER JOIN 
    #Colldata cv ON cv.VmPortfolioCode = isosec.VMCollateralPortfolioCode
LEFT JOIN -- Consider LEFT JOIN instead of INNER JOIN to prevent loss of records
    #Temp ON 
        #Temp.TransactionIdentificationUniqueTransactionIdentifier = iso.TransactionIdentificationUniqueTransactionIdentifier
        AND #Temp.ReportingTimestamp = iso.ReportingTimestamp
WHERE 
    trpt.jurisdiction = 'MAS' 
    AND isosec.VMCollateralPortfolioCode IS NOT NULL 
    AND ActionType <> 'TERM'
ORDER BY 
    iso.ReportingTimestamp DESC;
