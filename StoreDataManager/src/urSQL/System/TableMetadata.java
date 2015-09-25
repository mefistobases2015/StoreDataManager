package urSQL.System;

import java.util.ArrayList;

public class TableMetadata
{
	protected String _TableName;
	protected ArrayList<TableAttribute> _TableColumns;
	protected TableAttribute _PrimaryKey;
	
	public TableMetadata(String pTableName)
	{
		this._TableName = pTableName;
	}
	
	public String getTableName() 
	{
		return _TableName;
	}

	public void setTableName(String pTableName) 
	{
		this._TableName = pTableName;
	}

	public ArrayList<TableAttribute> getTableColumns() 
	{
		return _TableColumns;
	}

	public void setTableColumns(ArrayList<TableAttribute> pTableColumns) 
	{
		this._TableColumns = pTableColumns;
	}

	public TableAttribute getPrimaryKey() 
	{
		return _PrimaryKey;
	}

	public void setPrimaryKey(TableAttribute pPrimaryKey) 
	{
		this._PrimaryKey = pPrimaryKey;
	}
}
