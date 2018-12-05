
public class DatabaseObject
{
	private int value;
	private String name;
	private boolean modified;

	public DatabaseObject(int value)
	{
		this.value = value;
		this.modified = false;
	}
	
	public DatabaseObject(int value, String name)
	{
		this.value = value;
		this.name = name;
		this.modified = false;
	}
	
	public DatabaseObject(int value, String name, boolean mod)
	{
		this.value = value;
		this.name = name;
		this.modified = mod;
	}

	public int getValue()
	{
		return this.value;
	}

	public void setValue(int value)
	{
		this.value = value;
		this.modified = true;
	}
	
	public String getName()
	{
		return this.name;
	}

	public void setName(String name)
	{
		this.name = name;
	}
	public boolean isModified()
	{
		return modified;
	}
}
