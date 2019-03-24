/*****************************************************************************************
DATA SKEW INVESTIGATIGATION - HOW TO
*****************************************************************************************/

/*****************************************************************************************
1. DBCC PDW_SHOWSPACEUSED COMMAND
*****************************************************************************************/


DBCC PDW_SHOWSPACEUSED('CCI_FactSales_Non_Skewed')
GO


DBCC PDW_SHOWSPACEUSED('CCI_FactSales_Skewed')
GO


/*****************************************************************************************
2. dm_pdw_nodes_db_partition_stats Views
*****************************************************************************************/


--Faster than DBCC but less precise
SELECT  SCHEMA_NAME(t.schema_id) TABLE_SCHEMA
	,t.name TABLE_NAME
	,PDW_NODE_ID
	,SUM(B.RESERVED_SIZE_PAGES) RESERVED_SIZE_PAGES_PER_NODE
	,SUM(B.USED_SIZE_PAGES)		USED_SIZE_PAGES_PER_NODE
	,SUM(B.ROWS)				ROWS_PER_NODE
FROM sys.tables t
INNER JOIN 
(
 
SELECT 
	tm.object_id
	,nt.PDW_NODE_ID
	,nt.name  PHYS_NAME
	,SUM(ps.RESERVED_PAGE_COUNT) RESERVED_SIZE_PAGES
	,SUM(ps.USED_PAGE_COUNT)USED_SIZE_PAGES
	,SUM(ps.ROW_COUNT) ROWS
FROM
sys.pdw_table_mappings tm
	INNER JOIN sys.pdw_nodes_tables nt
		ON tm.physical_name = nt.name
	INNER JOIN sys.dm_pdw_nodes_db_partition_stats ps
		ON nt.object_id = ps.object_id
			AND nt.pdw_node_id = ps.pdw_node_id
			AND nt.distribution_id = ps.distribution_id
	WHERE ps.index_id < 2
GROUP BY tm.object_id, nt.NAME,nt.pdw_node_id
 
 ) B
	ON t.object_id = B.object_id
WHERE t.name IN ('CCI_FactSales_Non_Skewed','CCI_FactSales_Skewed')
GROUP BY t.schema_id,t.name,PDW_NODE_ID
GO

