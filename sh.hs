SELECT 
    TransactionIdentificationUniqueTransactionIdentifier, 
    MAX(TRY_CONVERT(DATETIME, 
        CASE 
            -- For the length 13 timestamp (e.g., '2024-09-19T08'), pad with ':00:00Z'
            WHEN LEN(ReportingTimestamp) = 13 THEN LEFT(ReportingTimestamp, 13) + ':00:00Z'
            
            -- For valid timestamps, keep them as is
            WHEN LEN(ReportingTimestamp) = 20 THEN ReportingTimestamp
            
            -- Handle NULL timestamps (you can decide to replace them or exclude them)
            WHEN ReportingTimestamp IS NULL THEN NULL
            
            ELSE ReportingTimestamp
        END, 127)
    ) AS ReportingTimestamp
INTO #Temp
FROM 
    onereg_main.ISOPrimaryPayload (NOLOCK) iso
INNER JOIN 
    onereg_main.trademessagerptjurisdiction (NOLOCK) trpt ON trpt.tmjid = iso.tmjid
INNER JOIN 
    onereg_main.ISOsecondaryPayload (NOLOCK) isosec ON isosec.TradeMessageISOPayloadID = iso.ISOPrimaryPayloadID
INNER JOIN 
    #Colldata ON #Colldata.VmPortfolioCode = isosec.VMCollateralPortfolioCode 
WHERE 
    trpt.jurisdiction = 'MAS'
    AND isosec.VMCollateralPortfolioCode IS NOT NULL
    AND TRY_CONVERT(DATETIME, 
        CASE 
            -- Same padding logic for filtering
            WHEN LEN(ReportingTimestamp) = 13 THEN LEFT(ReportingTimestamp, 13) + ':00:00Z'
            WHEN LEN(ReportingTimestamp) = 20 THEN ReportingTimestamp
            ELSE NULL
        END, 127
    ) <= '2024-08-08'
GROUP BY 
    TransactionIdentificationUniqueTransactionIdentifier;
