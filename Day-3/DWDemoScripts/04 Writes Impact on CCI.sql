/*****************************************************************************************
1. CHECKING CCI STATUS BEFORE WRITES
*****************************************************************************************/
SELECT
    s.name AS [Schema Name]
    ,t.name AS [Table Name]
    ,rg.partition_number AS [Partition Number]
    ,SUM(rg.total_rows) AS [Total Rows]
    ,SUM(CASE WHEN rg.State = 1 THEN rg.Total_rows Else 0 END) AS [Rows in OPEN Row Groups]
    ,SUM(CASE WHEN rg.State = 2 THEN rg.Total_Rows ELSE 0 END) AS [Rows in Closed Row Groups]
    ,SUM(CASE WHEN rg.State = 3 THEN rg.Total_Rows ELSE 0 END) AS [Rows in COMPRESSED Row Groups]
	,SUM(rg.Deleted_Rows) AS [Deleted Rows]
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
WHERE t.[name] = 'CCI_FactSales_WritesImpact'
GROUP BY s.name, t.name, rg.partition_number



/*****************************************************************************************
2. CHECKING PERFORMANCES WITH A SIMPLE QUERY AGAINST THE ORIGINAL TABLE
*****************************************************************************************/


DBCC DROPCLEANBUFFERS(ALL)
DBCC FREEPROCCACHE(ALL)
GO

DECLARE @Start DATETIME2 = GETDATE()

SELECT AVG(CAST(Qty AS BIGINT)) QTY_Avg
		,AVG(CAST(Amount AS BIGINT)) Amount_Avg
FROM CCI_FactSales_WritesImpact FR

DECLARE @End DATETIME2 = GETDATE()
SELECT DATEDIFF(ms,@Start,@End) [Not-Fragmented CCI ElapsedTime Ms]
GO


/*****************************************************************************************
3. DELETE APPROX 50% OF RECORDS
*****************************************************************************************/

DELETE FROM CCI_FactSales_WritesImpact
	WHERE idTerritory <= 122
GO

/*****************************************************************************************
4. CHECK THE CCI STATUS AFTER THE DELETE
*****************************************************************************************/

SELECT   tb.[name]                    AS [logical_table_name]
,        rg.[row_group_id]            AS [row_group_id]
,        rg.[state]                   AS [state]
,        rg.[state_desc]              AS [state_desc]
,        rg.[total_rows]              AS [total_rows]
,		 rg.[deleted_rows]			  AS [deleted_rows]
,        rg.[trim_reason_desc]        AS trim_reason_desc
,        mp.[physical_name]           AS physical_name
,		 CASE rg.[state] WHEN  1 THEN 0 ELSE  100 * (ISNULL(deleted_rows,0))/total_rows END AS 'RG Fragmentation %'  
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
WHERE tb.[name] = 'CCI_FactSales_WritesImpact'
ORDER BY 9 Desc
GO

/*****************************************************************************************
5. CHECKING PERFORMANCES AGAINST THE MODIFIED TABLE. 
DUE TO THE HIGH NUMBER OF HEAVILY FRAGMENTED ROW GROUPS IT IS JUST SLIGHTLY FASTER 
THAN THE FIRST ONE EVEN IF IT OWNS JUST A HALF THE RECORDS OF THE ORIGINAL.
HIGH FRAGMENTATION AFFECTS PERFORMANCES DUE TO UNNECESSARY READS 
*****************************************************************************************/

DBCC DROPCLEANBUFFERS(ALL)
DBCC FREEPROCCACHE(ALL)
GO

DECLARE @Start DATETIME2 = GETDATE()

SELECT AVG(CAST(Qty AS BIGINT)) QTY_Avg
		,AVG(CAST(Amount AS BIGINT)) Amount_Avg
FROM CCI_FactSales_WritesImpact 

DECLARE @End DATETIME2 = GETDATE()
SELECT DATEDIFF(ms,@Start,@End) [Fragmented CCI ElapsedTime Ms]
GO


/*****************************************************************************************
7. REBUILD THE FRAGMENTED TABLE SHOULD INCREASE PERFORMANCES
*****************************************************************************************/
--CTAS could be a valid maintenance command
ALTER TABLE CCI_FactSales_WritesImpact REBUILD 
GO

--Detailed view
SELECT   tb.[name]                    AS [logical_table_name]
,        rg.[row_group_id]            AS [row_group_id]
,        rg.[state]                   AS [state]
,        rg.[state_desc]              AS [state_desc]
,        rg.[total_rows]              AS [total_rows]
,		 rg.[deleted_rows]			  AS [deleted_rows]
,        rg.[trim_reason_desc]        AS trim_reason_desc
,        mp.[physical_name]           AS physical_name
,		 CASE rg.[state] WHEN  1 THEN 0 ELSE  100 * (ISNULL(deleted_rows,0))/total_rows END AS 'RG Fragmentation %'  
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
WHERE tb.[name] = 'CCI_FactSales_WritesImpact'
ORDER BY total_rows DESC
GO


/*****************************************************************************************
8. CHECKING PERFORMANCES WITH A SIMPLE QUERY AFTER THE REBUILD
*****************************************************************************************/

DBCC DROPCLEANBUFFERS(ALL)
DBCC FREEPROCCACHE(ALL)
GO

DECLARE @Start DATETIME2 = GETDATE()

SELECT AVG(CAST(Qty AS BIGINT)) QTY_Avg
		,AVG(CAST(Amount AS BIGINT)) Amount_Avg
FROM CCI_FactSales_WritesImpact

DECLARE @End DATETIME2 = GETDATE()
SELECT DATEDIFF(ms,@Start,@End) [CCI ElapsedTime Ms After Rebuild]
GO
