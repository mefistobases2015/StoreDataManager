package urSQL.System;

public class TableAttribute
{
	public static final String TYPE_CHAR = "CHAR";
	public static final String TYPE_VARCHAR = "VARCHAR";
	public static final String TYPE_INT = "INT";
	public static final String TYPE_DECIMAL = "DECIMAL";
	public static final String TYPE_DATETIME = "DATETIME";
	
	protected String _Name;
	protected String _Type;
	
	public TableAttribute(String pName, String pType)
	{
		this._Name = pName;
		this._Type = pType;
	}

	public String getName() 
	{
		return _Name;
	}

	public String getType() 
	{
		return _Type;
	}

	public void setType(String pType) 
	{
		this._Type = pType;
	}	

	public void setName(String pName) 
	{
		this._Name = pName;
	}
}
