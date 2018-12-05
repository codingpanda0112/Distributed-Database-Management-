
public interface DatabaseServerInterface
{
	public void startServer(String myIP, int myPort, String LookupServerIP, int LookupServerPort);
	public void addObject(String objName, int initialValue);
	public void stopServer();
}
