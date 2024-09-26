SELECT 
    TransactionIdentificationUniqueTransactionIdentifier, 
    MAX(TRY_CONVERT(DATETIME, 
        CASE 
            -- If the timestamp ends with 'T08', pad it with ':00:00Z' (for hours, minutes, and seconds)
            WHEN LEN(ReportingTimestamp) = 13 THEN LEFT(ReportingTimestamp, 13) + ':00:00Z'
            
            -- If the timestamp is more complete but missing seconds (e.g., '2024-09-19T08:10'), pad it with ':00Z'
            WHEN LEN(ReportingTimestamp) = 16 THEN LEFT(ReportingTimestamp, 16) + ':00Z'
            
            -- If it is already valid (has full time), use as is
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
            WHEN LEN(ReportingTimestamp) = 16 THEN LEFT(ReportingTimestamp, 16) + ':00Z'
            ELSE ReportingTimestamp
        END, 127
    ) <= '2024-08-08'
GROUP BY 
    TransactionIdentificationUniqueTransactionIdentifier;
