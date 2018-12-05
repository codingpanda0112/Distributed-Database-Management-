
public interface TransactionInterface
{
public String begin(String DatabaseServerIP, int DatabaseServerPort);
public int getValue(String objName);
public void setValue(String objName, int value);
public void commit();
public void rollback();
}