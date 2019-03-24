/**********************************************************************************************************
1. CHECK PERFORMANCES - Same Results but different performances
**********************************************************************************************************/

DBCC DROPCLEANBUFFERS(ALL)
DBCC FREEPROCCACHE(ALL)
GO

DECLARE @Start DATETIME2 = GETDATE()

SELECT 
	AVG(CAST(FS.Qty AS BIGINT) + CAST(FIS.Qty AS BIGINT)) AVG_Qty
FROM CCI_FactSales FS
	INNER JOIN CCI_FactInternetSales FIS
		ON FIS.idProduct = FS.idProduct
WHERE (	FIS.idProduct = CAST(-32555 AS BIGINT)
		AND FS.idProduct = CAST(-32555 AS BIGINT)
		AND FS.idTerritory = 100
		OR FIS.idseller = -15000
	)
GROUP BY  FIS.idproduct
HAVING COUNT_BIG(FIS.idproduct) >=  CAST(1 AS BIGINT)
OPTION(LABEL = 'Slow Query');

DECLARE @End DATETIME2 = GETDATE()
SELECT DATEDIFF(ms,@Start,@End) [Query #1]
GO


--Query #2
DBCC DROPCLEANBUFFERS(ALL)
DBCC FREEPROCCACHE(ALL)
GO

DECLARE @Start DATETIME2 = GETDATE();

WITH CCI_FactSales_Filtered AS (SELECT idproduct,Qty FROM CCI_FactSales WHERE idProduct = -32555 AND idTerritory = 100)
SELECT 
	AVG(CAST(FS.Qty AS BIGINT) + CAST(FIS.Qty AS BIGINT)) AVG_Qty
FROM CCI_FactSales_Filtered FS
	INNER JOIN CCI_FactInternetSales FIS
		ON FIS.idProduct = FS.idProduct
WHERE ( FIS.idProduct = CAST(-32555 AS BIGINT)
			OR FIS.idseller = CAST(-15000 AS BIGINT)
	)
GROUP BY  FIS.idproduct
HAVING COUNT_BIG(FIS.idproduct) >=  CAST(1 AS BIGINT)
OPTION(LABEL = 'Fast Query');

DECLARE @End DATETIME2 = GETDATE()
SELECT DATEDIFF(ms,@Start,@End) [Query #2]
GO

/**********************************************************************************************************
2. WHY Query#1 IS HEAVILY SLOWER THEN THE Query#2
**********************************************************************************************************/

set showplan_xml off
GO

--Check Both execution plans. Notice the Query#1 Use a Shuffle_move instead of a Partition_Move and needs to move more records.
EXPLAIN
SELECT 
	AVG(CAST(FS.Qty AS BIGINT) + CAST(FIS.Qty AS BIGINT)) AVG_Qty
FROM CCI_FactSales FS
	INNER JOIN CCI_FactInternetSales FIS
		ON FIS.idProduct = FS.idProduct
WHERE (	FIS.idProduct = CAST(-32555 AS BIGINT)
		AND FS.idProduct = CAST(-32555 AS BIGINT)
		AND FS.idTerritory = 100
		OR FIS.idseller = -15000
	)
GROUP BY  FIS.idproduct
HAVING COUNT_BIG(FIS.idproduct) >=  CAST(1 AS BIGINT);

EXPLAIN
WITH CCI_FactSales_Filtered AS (SELECT idproduct,Qty FROM CCI_FactSales WHERE idProduct = -32555 AND idTerritory = 100)
SELECT 
	AVG(CAST(FS.Qty AS BIGINT) + CAST(FIS.Qty AS BIGINT)) AVG_Qty
FROM CCI_FactSales_Filtered FS
	INNER JOIN CCI_FactInternetSales FIS
		ON FIS.idProduct = FS.idProduct
WHERE ( FIS.idProduct = CAST(-32555 AS BIGINT)
			OR FIS.idseller = CAST(-15000 AS BIGINT)
	)
GROUP BY  FIS.idproduct
HAVING COUNT_BIG(FIS.idproduct) >=  CAST(1 AS BIGINT)

/**********************************************************************************************************
3. WHILE THE FIRST ONE IS RUNNING OPEN ANOTHER SESSION AND RUN THE QUERY BELOW
**********************************************************************************************************/
--It will show the total number of steps using DMS, the total elapsed time and the DMS weight during the execution
SELECT TOP 50
		S.session_id
		,S.login_name
		,S.client_id
		,S.app_name
		,R.request_id
		,R.command request_command
		,R.[label]
		,COUNT(ST.request_id) steps_with_movement
		,SUM(ST.row_count) total_rows_moved
		,SUM(ST.total_elapsed_time) total_elapsed_dms_time
		,SUM(R.total_elapsed_time) total_elapsed_request_time
		,(SUM(ST.total_elapsed_time)*100.00)/SUM(R.total_elapsed_time) dms_time_percentage
FROM sys.dm_pdw_exec_requests R
	INNER JOIN sys.dm_pdw_request_steps ST
		ON R.request_id = ST.request_id
	INNER JOIN sys.dm_pdw_exec_sessions S
		ON R.session_id = S.session_id
WHERE (ST.location_type = 'DMS'
	OR ST.operation_type like '%Move%')
	AND R.status = 'Completed'
	--AND R.[label] Like '%Query'
GROUP BY 
		S.session_id
		,S.login_name
		,S.client_id
		,S.app_name
		,R.request_id
		,R.command
		,R.[label]
HAVING SUM(ST.row_count) > 10000
ORDER BY dms_time_percentage DESC
		,total_rows_moved DESC
GO
