import java.util.*;

public class TestScenario1
{
	public static final int LUSPort = 10007;
	public static final int NumDatabaseServers = 2;
	public static final int DBSFirstPort = 50000;
	
	public static void main(String args[])
	{
		if (args.length != 3)
		{
			System.out.println("usage: java TestScenario1 LookupServerClassName DatabaseServerClassName TransactionClassName");
			return; 
		}
		
		LookupServerInterface lus = null;
		Vector<DatabaseServerInterface> vdbs = new Vector<DatabaseServerInterface> ();
		
		try
		{
			/* initiation stage */
			Class lusClass = Class.forName(args[0]);
			lus = (LookupServerInterface) lusClass.newInstance();
			lus.startServer("127.0.0.1", LUSPort);
			
			Class dbsClass = Class.forName(args[1]);

			for (int i = 0; i < NumDatabaseServers; i++)
			{
				DatabaseServerInterface dbs = (DatabaseServerInterface) dbsClass.newInstance();
				vdbs.add(dbs);				
				dbs.startServer("127.0.0.1", DBSFirstPort + i, "127.0.0.1", LUSPort);
			}


			/* do the actual testing */

			Class transClass = Class.forName(args[2]);
			
			TransactionInterface T1 = (TransactionInterface) transClass.newInstance();
			String TID1 = T1.begin("127.0.0.1", DBSFirstPort);
			System.out.println("TID(T1)=" + TID1);
			
			TransactionInterface T2 = (TransactionInterface) transClass.newInstance();
			String TID2 = T2.begin("127.0.0.1", DBSFirstPort + 1);
			System.out.println("TID(T2)=" + TID2);
			
			
			System.out.println("Adding object \"sillyObject\" with initial value 89 on DB0...");
			(vdbs.get(0)).addObject("sillyObject", 89);
			System.out.println("done1");
			
						
			System.out.println("Reading sillyObject in T1: expected=89 ; got=" + T1.getValue("sillyObject"));
			
			System.out.print("Setting sillyObject to 91 in T1...");
			T1.setValue("sillyObject", 91);
			System.out.println("done");
			
			System.out.println("Reading sillyObject in T2: expected=89 ; got=" + T2.getValue("sillyObject"));

			System.out.print("Setting sillyObject to 100 in T2...");
			T2.setValue("sillyObject", 100);
			System.out.println("done");
					
			System.out.println("Reading sillyObject in T1: expected=91 ; got=" + T1.getValue("sillyObject"));
			System.out.println("Reading sillyObject in T2: expected=100 ; got=" + T2.getValue("sillyObject"));
			
			System.out.print("Commiting T1...");			
			T1.commit();
			System.out.println("done");
			
			System.out.print("Aborting T2...");						
			T2.rollback();
			System.out.println("done");

			TransactionInterface T3 = (TransactionInterface) transClass.newInstance();
			String TID3 = T3.begin("127.0.0.1", DBSFirstPort + 1);
			System.out.println("TID(T3)=" + TID3);
			System.out.println("Reading sillyObject in T3: expected=91 ; got=" + T3.getValue("sillyObject"));
	
			System.out.print("Commiting T3...");			
			T3.commit();
			System.out.println("done");

			/* termination stage */
			
			System.out.println("ending databases");
			for (int i = 0; i < NumDatabaseServers; i++)
			{
				DatabaseServerInterface dbs = vdbs.get(i);
				dbs.stopServer();
			}		

			System.out.println("ending lookup");
			lus.stopServer();
		}
		catch (Exception e)
		{
			System.err.println("error: " + e);
			return;
		}
	}
}
