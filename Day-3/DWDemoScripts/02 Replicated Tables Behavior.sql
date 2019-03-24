/*****************************************************************************************
1. CHECKING REPL TABLES STATUS AFTER ETL 
*****************************************************************************************/
SELECT t.[name]
	,c.state
	,p.distribution_policy_desc
FROM sys.tables t  
JOIN sys.pdw_replicated_table_cache_state c  
ON c.object_id = t.object_id 
JOIN sys.pdw_table_distribution_properties p 
ON p.object_id = t.object_id 
WHERE p.[distribution_policy_desc] = 'REPLICATE'
	AND t.[name] = 'DimProducts'
GO

/*****************************************************************************************
2. WHAT IF I RUN A QUERY AGAINST A REPL TABLE ?
*****************************************************************************************/
--Since the table is NotReady the engine will treat the DimProducts table as ROUND_ROBIN 
EXPLAIN	
SELECT AVG(CAST(Qty AS BIGINT)) QTY_Avg
		,AVG(CAST(Amount AS BIGINT)) Amount_Avg
FROM CCI_FactInternetSales FR
	INNER JOIN DimProducts P
		ON P.idProduct = FR.idProduct
GO

--This is the really first access to the DimProducts table. The engine will asynchronously create a copy of it on each compute node
SELECT AVG(CAST(Qty AS BIGINT)) QTY_Avg
		,AVG(CAST(Amount AS BIGINT)) Amount_Avg
FROM CCI_FactInternetSales FR
	INNER JOIN DimProducts P
		ON P.idProduct = FR.idProduct
GO

--The DimProducts table is now in the Ready status
SELECT t.[name]
	,c.state
	,p.distribution_policy_desc
FROM sys.tables t  
JOIN sys.pdw_replicated_table_cache_state c  
ON c.object_id = t.object_id 
JOIN sys.pdw_table_distribution_properties p 
ON p.object_id = t.object_id 
WHERE p.[distribution_policy_desc] = 'REPLICATE'
	AND t.[name] = 'DimProducts'
GO

--Since the table is Ready as replicate the engine will use a different and faster execution plan.
EXPLAIN
SELECT AVG(CAST(Qty AS BIGINT)) QTY_Avg
		,AVG(CAST(Amount AS BIGINT)) Amount_Avg
FROM CCI_FactInternetSales FR
	INNER JOIN DimProducts P
		ON P.idProduct = FR.idProduct
GO
/*****************************************************************************************
3. WHAT IF I MODIFY 1 ROW ?
*****************************************************************************************/

DELETE FROM DimProducts WHERE idProduct = (SELECT TOP 1 idProduct FROM DimProducts)
GO

--DimProducts should be NotReady since it has been changed and the copy is no longer valid
SELECT t.[name]
	,c.state
	,p.distribution_policy_desc
FROM sys.tables t  
JOIN sys.pdw_replicated_table_cache_state c  
ON c.object_id = t.object_id 
JOIN sys.pdw_table_distribution_properties p 
ON p.object_id = t.object_id 
WHERE p.[distribution_policy_desc] = 'REPLICATE'
	AND t.[name] = 'DimProducts'
GO

--Since the table is NotReady the engine will treat the DimProducts table as ROUND_ROBIN
EXPLAIN
SELECT AVG(CAST(Qty AS BIGINT)) QTY_Avg
		,AVG(CAST(Amount AS BIGINT)) Amount_Avg
FROM CCI_FactInternetSales FR
	INNER JOIN DimProducts P
		ON P.idProduct = FR.idProduct
GO


--Cache will be created Asynchronously
SELECT t.[name]
	,c.state
	,p.distribution_policy_desc
FROM sys.tables t  
JOIN sys.pdw_replicated_table_cache_state c  
ON c.object_id = t.object_id 
JOIN sys.pdw_table_distribution_properties p 
ON p.object_id = t.object_id 
WHERE p.[distribution_policy_desc] = 'REPLICATE'
GO

--Run this portion to make all the Replicated Table "Ready"
SELECT COUNT(*) FROM DimProducts
SELECT COUNT(*) FROM DimTerritories
SELECT COUNT(*) FROM DimSellers
SELECT COUNT(*) FROM CCI_DimProducts
SELECT COUNT(*) FROM RS_DimProducts
GO


--It could take a while to see all table Ready. It depends on their space allocation
SELECT t.[name]
	,c.state
	,p.distribution_policy_desc
FROM sys.tables t  
JOIN sys.pdw_replicated_table_cache_state c  
ON c.object_id = t.object_id 
JOIN sys.pdw_table_distribution_properties p 
ON p.object_id = t.object_id 
WHERE p.[distribution_policy_desc] = 'REPLICATE'
GO