/*****************************************************************************************
1. CHECKING PERFORMANCES
*****************************************************************************************/

/*****************************************************************************************
CCI Table with few rows
*****************************************************************************************/

DBCC DROPCLEANBUFFERS(ALL)
DBCC FREEPROCCACHE(ALL)
GO

DECLARE @Start DATETIME2 = GETDATE()


SELECT AVG(CAST(Qty AS BIGINT)) QTY_Avg
		,AVG(CAST(Amount AS BIGINT)) Amount_Avg
FROM CCI_FactSales_FewRows FR
WHERE id >= 2500000

DECLARE @End DATETIME2 = GETDATE()
SELECT DATEDIFF(ms,@Start,@End) [CCI ElapsedTime Ms]
GO

/*****************************************************************************************
RS Table
*****************************************************************************************/

DBCC DROPCLEANBUFFERS(ALL)
DBCC FREEPROCCACHE(ALL)
GO

DECLARE @Start DATETIME2 = GETDATE()

SELECT AVG(CAST(Qty AS BIGINT)) QTY_Avg
		,AVG(CAST(Amount AS BIGINT)) Amount_Avg
FROM RS_FactSales_FewRows FR
WHERE id >= 2500000

DECLARE @End DATETIME2 = GETDATE()
SELECT DATEDIFF(ms,@Start,@End) [RS ElapsedTime Ms]
GO

/*************************************************************************************************
2. WHY ?
**************************************************************************************************/

/*************************************************************************************************
Check CCI table with few rows
**************************************************************************************************/

SELECT
    s.name AS [Schema Name]
    ,t.name AS [Table Name]
    ,rg.partition_number AS [Partition Number]
    ,SUM(rg.total_rows) AS [Total Rows]
    ,SUM(CASE WHEN rg.State = 1 THEN rg.Total_rows Else 0 END) AS [Rows in OPEN Row Groups]
    ,SUM(CASE WHEN rg.State = 2 THEN rg.Total_Rows ELSE 0 END) AS [Rows in Closed Row Groups]
    ,SUM(CASE WHEN rg.State = 3 THEN rg.Total_Rows ELSE 0 END) AS [Rows in COMPRESSED Row Groups]
FROM sys.pdw_nodes_column_store_row_groups rg
	JOIN sys.pdw_nodes_tables pt
		ON rg.object_id = pt.object_id 
			AND rg.pdw_node_id = pt.pdw_node_id 
				AND pt.distribution_id = rg.distribution_id
	JOIN sys.pdw_table_mappings tm
		ON pt.name = tm.physical_name
	INNER JOIN sys.tables t
		ON tm.object_id = t.object_id
	INNER JOIN sys.schemas s
		ON t.schema_id = s.schema_id
WHERE t.[name] = 'CCI_FactSales_FewRows'
GROUP BY s.name, t.name, rg.partition_number


/*************************************************************************************************
Check CCI Detailed Status
**************************************************************************************************/
SELECT   tb.[name]                    AS [logical_table_name]
,        rg.[row_group_id]            AS [row_group_id]
,        rg.[state]                   AS [state]
,        rg.[state_desc]              AS [state_desc]
,        rg.[total_rows]              AS [total_rows]
,        rg.[trim_reason_desc]        AS trim_reason_desc
,        mp.[physical_name]           AS physical_name
FROM    sys.[schemas] sm
	JOIN sys.[tables] tb               
		ON  sm.[schema_id] = tb.[schema_id]                             
	JOIN sys.[pdw_table_mappings] mp   
		ON  tb.[object_id]          = mp.[object_id]
	JOIN sys.[pdw_nodes_tables] nt     
		ON  nt.[name] = mp.[physical_name]
	JOIN sys.[dm_pdw_nodes_db_column_store_row_group_physical_stats] rg      
		ON  rg.[object_id] = nt.[object_id]
		AND rg.[pdw_node_id] = nt.[pdw_node_id]
		AND rg.[distribution_id] = nt.[distribution_id]   
WHERE tb.[name] = 'CCI_FactSales_FewRows'
ORDER BY 1,2,7
GO

/*************************************************************************************************
3. WHAT IF I HAVE AT LEAST 60 MLN ROWS IN MY CCI ?
**************************************************************************************************/

/*****************************************************************************************
CCI Table with 63Mln Rows
*****************************************************************************************/

DBCC DROPCLEANBUFFERS(ALL)
DBCC FREEPROCCACHE(ALL)
GO

DECLARE @Start DATETIME2 = GETDATE()

SELECT AVG(CAST(Qty AS BIGINT)) QTY_Avg
		,AVG(CAST(Amount AS BIGINT)) Amount_Avg
FROM CCI_FactSales 
WHERE id >= 2500000

DECLARE @End DATETIME2 = GETDATE()
SELECT DATEDIFF(ms,@Start,@End) [CCI ElapsedTime Ms]
GO

/*****************************************************************************************
RS Table
*****************************************************************************************/


DBCC DROPCLEANBUFFERS(ALL)
DBCC FREEPROCCACHE(ALL)
GO

DECLARE @Start DATETIME2 = GETDATE()

SELECT AVG(CAST(Qty AS BIGINT)) QTY_Avg
		,AVG(CAST(Amount AS BIGINT)) Amount_Avg
FROM RS_FactSales 
WHERE id >= 2500000

DECLARE @End DATETIME2 = GETDATE()
SELECT DATEDIFF(ms,@Start,@End) [RS ElapsedTime Ms]
GO


/*************************************************************************************************
Check CCI table with 63Mln rows
**************************************************************************************************/

SELECT
    s.name AS [Schema Name]
    ,t.name AS [Table Name]
    ,rg.partition_number AS [Partition Number]
    ,SUM(rg.total_rows) AS [Total Rows]
    ,SUM(CASE WHEN rg.State = 1 THEN rg.Total_rows Else 0 END) AS [Rows in OPEN Row Groups]
    ,SUM(CASE WHEN rg.State = 2 THEN rg.Total_Rows ELSE 0 END) AS [Rows in Closed Row Groups]
    ,SUM(CASE WHEN rg.State = 3 THEN rg.Total_Rows ELSE 0 END) AS [Rows in COMPRESSED Row Groups]
FROM sys.pdw_nodes_column_store_row_groups rg
	JOIN sys.pdw_nodes_tables pt
		ON rg.object_id = pt.object_id 
			AND rg.pdw_node_id = pt.pdw_node_id 
				AND pt.distribution_id = rg.distribution_id
	JOIN sys.pdw_table_mappings tm
		ON pt.name = tm.physical_name
	INNER JOIN sys.tables t
		ON tm.object_id = t.object_id
	INNER JOIN sys.schemas s
		ON t.schema_id = s.schema_id
WHERE t.[name] = 'CCI_FactSales'
GROUP BY s.name, t.name, rg.partition_number


/*************************************************************************************************
Check CCI Detailed Status
**************************************************************************************************/
SELECT   tb.[name]                    AS [logical_table_name]
,        rg.[row_group_id]            AS [row_group_id]
,        rg.[state]                   AS [state]
,        rg.[state_desc]              AS [state_desc]
,        rg.[total_rows]              AS [total_rows]
,        rg.[trim_reason_desc]        AS trim_reason_desc
,        mp.[physical_name]           AS physical_name
FROM    sys.[schemas] sm
	JOIN sys.[tables] tb               
		ON  sm.[schema_id] = tb.[schema_id]                             
	JOIN sys.[pdw_table_mappings] mp   
		ON  tb.[object_id]          = mp.[object_id]
	JOIN sys.[pdw_nodes_tables] nt     
		ON  nt.[name] = mp.[physical_name]
	JOIN sys.[dm_pdw_nodes_db_column_store_row_group_physical_stats] rg      
		ON  rg.[object_id] = nt.[object_id]
		AND rg.[pdw_node_id] = nt.[pdw_node_id]
		AND rg.[distribution_id] = nt.[distribution_id]   
WHERE tb.[name] = 'CCI_FactSales'
ORDER BY 1,2,7
GO


/*************************************************************************************************
4. WHAT IF THE CCI TABLE IS OVERPARTITIONED ?
**************************************************************************************************/

/*****************************************************************************************
CCI Table with 63Mln Rows OverPartitioned
*****************************************************************************************/

DBCC DROPCLEANBUFFERS(ALL)
DBCC FREEPROCCACHE(ALL)
GO

DECLARE @Start DATETIME2 = GETDATE()

SELECT AVG(CAST(Qty AS BIGINT)) QTY_Avg
		,AVG(CAST(Amount AS BIGINT)) Amount_Avg
FROM CCI_FactSales_OverPartitioned 
WHERE id >= 2500000

DECLARE @End DATETIME2 = GETDATE()
SELECT DATEDIFF(ms,@Start,@End) [CCI ElapsedTime Ms]
GO

/*****************************************************************************************
CCI Table with 63Mln Rows 
*****************************************************************************************/

DBCC DROPCLEANBUFFERS(ALL)
DBCC FREEPROCCACHE(ALL)
GO

DECLARE @Start DATETIME2 = GETDATE()

SELECT AVG(CAST(Qty AS BIGINT)) QTY_Avg
		,AVG(CAST(Amount AS BIGINT)) Amount_Avg
FROM CCI_FactSales F
WHERE id >= 2500000

DECLARE @End DATETIME2 = GETDATE()
SELECT DATEDIFF(ms,@Start,@End) [CCI ElapsedTime Ms]
GO


/*************************************************************************************************
Check CCI table with 63Mln rows Overpartitioned
**************************************************************************************************/

SELECT
    s.name AS [Schema Name]
    ,t.name AS [Table Name]
    ,rg.partition_number AS [Partition Number]
    ,SUM(rg.total_rows) AS [Total Rows]
    ,SUM(CASE WHEN rg.State = 1 THEN rg.Total_rows Else 0 END) AS [Rows in OPEN Row Groups]
    ,SUM(CASE WHEN rg.State = 2 THEN rg.Total_Rows ELSE 0 END) AS [Rows in Closed Row Groups]
    ,SUM(CASE WHEN rg.State = 3 THEN rg.Total_Rows ELSE 0 END) AS [Rows in COMPRESSED Row Groups]
FROM sys.pdw_nodes_column_store_row_groups rg
	JOIN sys.pdw_nodes_tables pt
		ON rg.object_id = pt.object_id 
			AND rg.pdw_node_id = pt.pdw_node_id 
				AND pt.distribution_id = rg.distribution_id
	JOIN sys.pdw_table_mappings tm
		ON pt.name = tm.physical_name
	INNER JOIN sys.tables t
		ON tm.object_id = t.object_id
	INNER JOIN sys.schemas s
		ON t.schema_id = s.schema_id
WHERE t.[name] = 'CCI_FactSales_OverPartitioned'
GROUP BY s.name, t.name, rg.partition_number


/*************************************************************************************************
Check CCI Detailed Status
**************************************************************************************************/
SELECT   tb.[name]                    AS [logical_table_name]
,        rg.[row_group_id]            AS [row_group_id]
,        rg.[state]                   AS [state]
,        rg.[state_desc]              AS [state_desc]
,        rg.[total_rows]              AS [total_rows]
,        rg.[trim_reason_desc]        AS trim_reason_desc
,        mp.[physical_name]           AS physical_name
FROM    sys.[schemas] sm
	JOIN sys.[tables] tb               
		ON  sm.[schema_id] = tb.[schema_id]                             
	JOIN sys.[pdw_table_mappings] mp   
		ON  tb.[object_id]          = mp.[object_id]
	JOIN sys.[pdw_nodes_tables] nt     
		ON  nt.[name] = mp.[physical_name]
	JOIN sys.[dm_pdw_nodes_db_column_store_row_group_physical_stats] rg      
		ON  rg.[object_id] = nt.[object_id]
		AND rg.[pdw_node_id] = nt.[pdw_node_id]
		AND rg.[distribution_id] = nt.[distribution_id]   
WHERE tb.[name] = 'CCI_FactSales_OverPartitioned'
ORDER BY 1,2,7
GO