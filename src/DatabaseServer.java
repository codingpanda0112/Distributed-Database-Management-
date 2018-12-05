
import java.nio.*;
import java.nio.channels.*;
import java.net.*;
import java.io.*;
import java.util.*;
import java.nio.channels.spi.*;
import java.util.concurrent.*;
import org.apache.log4j.Logger;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.FileAppender;
import org.apache.log4j.*;


public class DatabaseServer extends Thread implements DatabaseServerInterface{

	//public static final int SERVER_PORT = 5555;
	//public ServerSocketChannel ssChannel;
	//public Selector selector = null;
	
	String ip;
	int port;
	String LookupServerIP;
	int LookupServerPort;
	int poolSize;
	private ServerSocketChannel ssChannel;
	private Selector selector = null;
	private ExecutorService pool;
	//objects
	HashMap<String, Integer> DB;
	//present clients<TID, modified object list>
	HashMap<String, Data> CP;
	//cache for servers ( nu for values but for sending changes from clients)
	HashMap<String, Server> Cache; //<objectname, information aboutServer>
	Object sinc = new Object();
	File f;
	FileAppender fapp;
	Logger logger;
	
	class Data{
		public LinkedList<DatabaseObject> list;
		public SocketChannel sChannel;
		public Data(SocketChannel sChannel, LinkedList<DatabaseObject> list)
		{
			this.sChannel = sChannel;
			this.list = list;
		}
		public void add(DatabaseObject o)
		{
			for(DatabaseObject d : list)
			{
				if(d.getName().equals(o.getName()))
				{
					d.setValue(o.getValue());
					return;
				}				
			}
			list.add(o);
		}
	}
	
	public void startServer(String myIP, int myPort, String LookupServerIP, int LookupServerPort)
	{
		ip=myIP;
		port=myPort;
		poolSize = 4;
		this.LookupServerIP = LookupServerIP;
		this.LookupServerPort = LookupServerPort;
		DB = new HashMap<String, Integer>();
		CP = new HashMap<String, Data>();
		Cache = new HashMap<String, Server>();
		
		try
		{
			logger = Logger.getLogger(DatabaseServer.class);
			f = new File("logs", "DatabaseServer"+myIP+myPort+".log");
			fapp = new FileAppender(new TTCCLayout("DATE"), f.getAbsolutePath());
			logger.addAppender(fapp);
			BasicConfigurator.configure();
			logger.setLevel((Level)Level.ALL);
			logger.info("Entering DatabaseServer");
		}
		catch(IOException e)
		{
			System.err.println("Unable to open logger");
		}
		
		try
		{
			//Create a non-blocking server socket and check for connections
			ssChannel = ServerSocketChannel.open();
			ssChannel.configureBlocking(false);
			ssChannel.socket().bind(new InetSocketAddress(port));
			pool = Executors.newFixedThreadPool(poolSize);
			
			//Create a selector and register a socket channel
			// Create the selector
			selector = Selector.open();
			
			// Register the channel with selector, listening for all events
			ssChannel.register(selector, ssChannel.validOps());
		
			if (selector==null) logger.info("Selector null");
			else
			{
					//can be run
			}
		}
		catch(IOException e)
		{
			logger.info("Exception while starting DatabaseServer");
			e.printStackTrace();
		}
		this.start();
		
	}
	
	public void addObject(String objName, int initialValue)
	{
		synchronized(DB)
		{
			DB.put(objName, initialValue);
		}
		informLookup(objName);
	}
	
	public void stopServer()
	{
		try {
		 Thread.sleep(1000);
		 //pool.shutdown(); // Disable new tasks from being submitted
		}
		catch(InterruptedException ie)
		{
			pool.shutdownNow();
		     // Preserve interrupt status
		     Thread.currentThread().interrupt();
		}
		catch(java.util.concurrent.RejectedExecutionException e)
		{
			logger.info("DB Server shutting down, no more tasks being submitted");
		}
		 try {
		     // Wait a while for existing tasks to terminate
		     if (!pool.awaitTermination(1, TimeUnit.SECONDS)) {
		       pool.shutdownNow(); 
		       // Cancel currently executing tasks
		       // Wait a while for tasks to respond to being cancelled
		       if (!pool.awaitTermination(1, TimeUnit.SECONDS))
		    	   logger.info("Pool did not terminate");
		     }
		     selector.close();
		     ssChannel.close();
		   } 
		 catch (InterruptedException ie) {
		     // (Re-)Cancel if current thread also interrupted
		     pool.shutdownNow();
		     // Preserve interrupt status
		     Thread.currentThread().interrupt();
		   }
		 catch(Exception e){}
	}
	
	public void run()
	{
		int a;
		while (true) 
		{
			try
			{
				// Wait for an event
				a = selector.select();				
				if(a == 0)
				{
					synchronized(sinc)
					{
					//Thread.sleep(200);
					}
					continue;
				} 
					
				if(selector == null || !selector.isOpen())
					break;
				// Get list of selection keys with pending events
				Iterator it = selector.selectedKeys().iterator();
			
				// Process each key at a time
				while (it.hasNext()) {
					// Get the selection key
					SelectionKey selKey = (SelectionKey)it.next();
				
					// Remove it from the list to indicate that it is being processed
					it.remove();
					
					pool.execute(new HandlerData(selKey, this.selector));
					
				}
			}
			catch (IOException e) 
			{	// Handle error with selector
				logger.info("IO: Ending DB server");
				break;
			}
			catch(ClosedSelectorException e)
			{
				logger.info("Closed Selector: Ending DB server");
				break;
			}
			catch(RejectedExecutionException e)
			{
				logger.info("Thread Pool Unavailable : Ending DB server");
				break;
			}
			catch(Exception e)
			{
				logger.info("Ending DB server");
				break;
			}
		}
	}
	
	
	//request info for a client
	public void requestInfoLookup(String objName, String TID)
	{
		SocketChannel sChannel = connectToLookup();
		
		if(sChannel == null)
		{
			logger.info("null socket");return;
		}
		
		ByteBuffer buf = ByteBuffer.allocateDirect(1024);
		try 
		{
			synchronized(sinc)
			{
				this.selector.wakeup();
				sChannel.register(this.selector, SelectionKey.OP_READ); //vreau sa astept raspuns
			}
			buf.clear();
			int numBytesRead;
			
			//Fill the buffer with the bytes to write;
			// see e160 Putting Bytes into a ByteBuffer
			String message = "I"+objName+" "+TID+"\0";
			buf.put(message.getBytes());
			// Prepare the buffer for reading by the socket
			buf.flip();
		
			int numToWrite = message.getBytes().length, wr = 0;
			while(wr<numToWrite)
			{
				int numBytesWritten = sChannel.write(buf);
				wr+=numBytesWritten;
			}
			logger.info("DB wrote info request "+wr);
		} 
		catch (IOException e) {}		
	}
	
	//request info for a client, in order to send a request to modify that object
	public void requestInfoLookupModify(String objName, String val)
	{
		SocketChannel sChannel = connectToLookup();
		
		if(sChannel == null)
		{
			logger.info("null socket");return;
		}
		
		ByteBuffer buf = ByteBuffer.allocateDirect(1024);
		try 
		{
			synchronized(sinc)
			{
				this.selector.wakeup();
				sChannel.register(this.selector, SelectionKey.OP_READ);
				 //I want to wait for an answer
			}
			buf.clear();
			int numBytesRead;
			
			//Fill the buffer with the bytes to write;
			String message = "J"+objName+" "+val+"\0";
			buf.put(message.getBytes());
			// Prepare the buffer for reading by the socket
			buf.flip();
		
			int numToWrite = message.getBytes().length, wr = 0;
			while(wr<numToWrite)
			{
				int numBytesWritten = sChannel.write(buf);
				wr+=numBytesWritten;
			}
			logger.info("DB wrote info request for modify"+wr);
		} 
		catch (IOException e) {}		
	}
	
	//I added a new object, send notification to the lookup server
	public void informLookup(String objName)
	{
		SocketChannel sChannel = connectToLookup();
		if(sChannel == null)
		{
			logger.info("null socket");return;
		}
		
		ByteBuffer buf = ByteBuffer.allocateDirect(1024);
		try 
		{
			buf.clear();
			int numBytesRead;
			
			//Fill the buffer with the bytes to write;
			String message = "A"+objName+" "+ip+" "+(new Integer(port)).toString()+"\0";			
			buf.put(message.getBytes());
			// Prepare the buffer for reading by the socket
			buf.flip();
		
			// Write bytes
			int numToWrite = message.getBytes().length, wr = 0;
			while(wr<numToWrite)
			{
				int numBytesWritten = sChannel.write(buf);
				wr+=numBytesWritten;
			}
			logger.info("DB adding info "+wr);
		} 
		catch (IOException e) {}		
	}
		
	
	public SocketChannel connectToOther(String DBIP, int DBPort)
	{
		SocketChannel sChannel = null;
		// Create a non-blocking socket and check for connections
		try 
		{
			// Create a non-blocking socket channel on port 80
			sChannel = createSocketChannel(DBIP,DBPort);
	
			// Before the socket is usable, the connection must be completed
			// by calling finishConnect(), which is non-blocking
			while (!sChannel.finishConnect()) 
			{
				// Do something else
			}
			// Socket channel is now ready to use
			logger.info("Connected to other");			
		} 
		catch (IOException e) {}
		return sChannel;
	}
	
	public SocketChannel connectToLookup()
	{
		SocketChannel sChannel = null;
		// Create a non-blocking socket and check for connections
		try 
		{
			// Create a non-blocking socket channel on port 80
			sChannel = createSocketChannel(LookupServerIP,LookupServerPort);
	
			// Before the socket is usable, the connection must be completed
			// by calling finishConnect(), which is non-blocking
			while (!sChannel.finishConnect()) 
			{
				// Do something else
			}
			// Socket channel is now ready to use
			logger.info("Connected to lookup");			
		} 
		catch (IOException e) {}
		return sChannel;
	}
	
	public  SocketChannel createSocketChannel(String hostName, int port) throws IOException {
        // Create a non-blocking socket channel
	SocketChannel sChannel = SocketChannel.open();
	sChannel.configureBlocking(false);
    
        // Send a connection request to the server; this method is non-blocking
		sChannel.connect(new InetSocketAddress(hostName, port));
		return sChannel;
	}

class HandlerData implements Runnable {
	   private final SelectionKey selKey;
	   private Selector selector;
	   HandlerData(SelectionKey selKey, Selector selector) { this.selKey = selKey; this.selector = selector;}
	   
	   public void run() 
	   {		   
	     // read and service request on socket
		 try{
				if (selKey.isValid() && selKey.isConnectable())
				{
					// Get channel with connection request
					SocketChannel sChannel = (SocketChannel)selKey.channel();
				
					boolean success = sChannel.finishConnect();
					if (!success) {
						// An error occurred; handle it			
						// Unregister the channel with this selector
						selKey.cancel();
					}
				}
				
				if (selKey.isValid() && selKey.isReadable()) 
				{
					// Get channel with bytes to read
					SocketChannel sChannel = (SocketChannel)selKey.channel();
					ByteBuffer buf = ByteBuffer.allocateDirect(1024);
										
					int numBytesRead;
					//while((numBytesRead = sChannel.read(buf))<1) ;
					numBytesRead = sChannel.read(buf);
					
					if (numBytesRead == -1) 
					{
						sChannel.close();
						selKey.cancel();
					} 
					else 
					{
						buf.flip();
						// Read the bytes from the buffer ...;
						if(numBytesRead>0)
						{
							int contor = 0;
							while(contor < numBytesRead)
							{
								if((char)buf.get(contor) == 'L')
								{
									if((char)buf.get(contor+1) == 'A')
									{
										logger.info("DB->Got lookup answer");
										String n = new String(""); //name of the object I'm serching for
										String portadd = new String("");//port of the server that has it
										String ipadd = new String("");//ip of the server  that has it
										String TID = new String("");//client TID that is asking for the info
										int i = contor+2;
										while(true)
										{
											char c = (char)buf.get(i++);
											if(c == ' ')
												break;
											n = n + c;
										}
										
										while(true)
										{
											char c = (char)buf.get(i++);
											if(c == ' ')
												break;
											ipadd = ipadd + c;
										}
										
										while(true)
										{
											char c = (char)buf.get(i++);
											if(c == ' ')
												break;
											portadd = portadd + c;
										}
										
										while(true)
										{
											char c = (char)buf.get(i++);
											if(c == '\0')
												break;
											TID = TID + c;
										}
										contor=i;
										sChannel.close();
										selKey.cancel();
										synchronized(Cache)
										{
											if(Cache.get(n) == null)
											{
												logger.info("Added to cache: "+n);
												Cache.put(n, new Server(ipadd, Integer.parseInt(portadd)));
											}
										}
										
										sChannel = connectToOther(ipadd, Integer.parseInt(portadd));
										String message = "DG"+n+" "+TID+"\0";
										DBWrite(message, sChannel);
										ByteBuffer buf2 = ByteBuffer.allocateDirect(1024);
										logger.info("DB wrote request to other DB ");
										try 
										{										
											synchronized(sinc)
											{
												this.selector.wakeup();
												sChannel.register(this.selector, SelectionKey.OP_READ); //I want to wait for an answer
											}
										} 
										catch (IOException e) {}
									}
									else if((char)buf.get(contor+1) == 'B')
									{
										logger.info("DB->Got lookup answer-sending modify");
										String n = new String(""); 
										String portadd = new String("");
										String ipadd = new String("");
										String val = new String("");
										int i = contor+2;
										while(true)
										{
											char c = (char)buf.get(i++);
											if(c == ' ')
												break;
											n = n + c;
										}
										
										while(true)
										{
											char c = (char)buf.get(i++);
											if(c == ' ')
												break;
											ipadd = ipadd + c;
										}
										
										while(true)
										{
											char c = (char)buf.get(i++);
											if(c == ' ')
												break;
											portadd = portadd + c;
										}
										
										while(true)
										{
											char c = (char)buf.get(i++);
											if(c == '\0')
												break;
											val = val + c;
										}
										contor=i;
										sChannel.close();
										selKey.cancel();
										synchronized(Cache)
										{
											if(Cache.get(n) == null)
											{
												logger.info("Added to cache: "+n);
												Cache.put(n, new Server(ipadd, Integer.parseInt(portadd)));
											}
										}
										
										sChannel = connectToOther(ipadd, Integer.parseInt(portadd));
										String message = "DM"+n+" "+val+"\0";
										DBWrite(message, sChannel);										
										logger.info("DB wrote request to modify to other DB ");										
									}
								}
								else if((char)buf.get(contor) == 'D')
								{
										 if((char)buf.get(contor+1) == 'G')
										 {
											 logger.info("DB->Got other DB request");
											String n = new String(""); 
											String TID = new String("");
											int i = contor+2;
											while(true)
											{
											 char c = (char)buf.get(i++);
											 if(c == ' ')
												 break;
											 n = n + c;
											}
											while(true)
											{
											 char c = (char)buf.get(i++);
											 if(c == '\0')
												 break;
											 TID = TID + c;
											}
											contor=i;
											ByteBuffer buf2 = ByteBuffer.allocateDirect(1024);
											if(DB.get(n) == null) logger.info("N-am obiectul cerut!!!");
											else
											{
												String message = "DA"+n+" "+DB.get(n)+" "+TID+"\0";
												DBWrite(message, sChannel);
												logger.info("DB wrote answer to other DB ");
											}
										 }
										 else if((char)buf.get(contor+1) == 'A')
										 {
											 logger.info("DB->Got other DB answer");
												String n = new String("");
												String val = new String("");
												String TID = new String("");
												int i = contor+2;
												while(true)
												{
												 char c = (char)buf.get(i++);
												 if(c == ' ')
													 break;
												 n = n + c;
												}
												while(true)
												{
												 char c = (char)buf.get(i++);
												 if(c == ' ')
													 break;
												 val = val + c;
												}
												while(true)
												{
												 char c = (char)buf.get(i++);
												 if(c == '\0')
													 break;
												 TID = TID + c;
												}
												contor=i;
												
												//send an answer to the client with tid=TID; the client socket can be found in Data
												logger.info("Print  altDBserver info des: "+n+" value "+val);
												sChannel.close();
												selKey.cancel();
												
												if(CP.get(TID) == null)
												{
													logger.info("error trasmisie(DB), TIDexit ");
												}
												else
												{
													String message = "A"+val+"\0";
													DBWrite(message, CP.get(TID).sChannel);
												}
										}
										else if((char)buf.get(contor+1) == 'M')
										{
												logger.info("DB->Got DB request to modify");
												String n = new String(""); 
												String val = new String("");
												int i = contor+2;
												while(true)
												{
												 char c = (char)buf.get(i++);
												 if(c == ' ')
													 break;
												 n = n + c;
												}
												while(true)
												{
												 char c = (char)buf.get(i++);
												 if(c == '\0')
													 break;
												 val = val + c;
												}
												contor=i;
												synchronized(DB)
												{
													DB.put(n, Integer.parseInt(val));
												}
												sChannel.close();
												selKey.cancel();
										}
									
								}
								else if((char)buf.get(contor) == 'C')
								{
									//requests from clients
									if((char)buf.get(contor+1) == 'C')
									{
										String TID_s = ip + port + (new Integer((int)(Math.random()*2000)).toString())  + ip + port;
										synchronized(CP)
										{
											CP.put(TID_s, new Data(sChannel, new LinkedList<DatabaseObject>()));
										}
										logger.info("Sending TID: "+TID_s);
										String message ="C"+TID_s+"\0";
										DBWrite(message, sChannel);
										contor+=3;
									}
									else if((char)buf.get(contor+1) == 'G')
									{
										String TID_s = new String("");
										String n = new String("");
										int i = contor+2;
										while(true)
										{
										 char c = (char)buf.get(i++);
										 if(c == ' ')
											 break;
										 TID_s = TID_s + c;
										}
										while(true)
										{
										 char c = (char)buf.get(i++);
										 if(c == '\0')
											 break;
										 n = n + c;
										}
										
										Data dbo;
										if((dbo=CP.get(TID_s)) == null)
										{
											logger.info("Transmission error(CL-G), TID-ul "+TID_s+" does not exist!");
										}
										else
										{
											if(DB.get(n) != null)
											{
												String message ="A"+DB.get(n)+"\0";
												DBWrite(message, sChannel);
											}
											else
											{
												//send request to lookup
												requestInfoLookup(n, TID_s);
											}
										}
										contor = i;
									}
									else if((char)buf.get(contor+1) == 'S')
									{
										String TID_s = new String("");
										String n = new String("");
										String val = new String("");
										int i = contor+2;
										while(true)
										{
										 char c = (char)buf.get(i++);
										 if(c == ' ')
											 break;
										 TID_s = TID_s + c;
										}
										while(true)
										{
										 char c = (char)buf.get(i++);
										 if(c == ' ')
											 break;
										 n = n + c;
										}
										while(true)
										{
										 char c = (char)buf.get(i++);
										 if(c == '\0')
											 break;
										 val = val + c;
										}
										
										Data dbo;
										if((dbo=CP.get(TID_s)) == null)
										{
											logger.info("Transmission error(CL-S), the TID required does not exist!");
										}
										else
										{
											dbo.add(new DatabaseObject(Integer.parseInt(val), n, true));
										}
										contor = i;
									}
									else if((char)buf.get(contor+1) == 'M')
									{
										logger.info("DB-> commit");
										
										String TID_s = new String("");
										int i = contor+2;
										while(true)
										{
										 char c = (char)buf.get(i++);
										 if(c == '\0')
											 break;
										 TID_s = TID_s + c;
										}
										
										Data dbo;
										if((dbo = CP.get(TID_s)) == null)
										{
											logger.info("Transmission error(CL-M), the TID required does not exist!");
										}
										else
										{
											for(DatabaseObject o : dbo.list)
											{
												if(DB.get(o.getName()) != null)
												{
													logger.info("DB Modifying my own object");
													synchronized(DB)
													{
														DB.put(o.getName(), o.getValue());
													}
												}
												else
												{
													Server sv = Cache.get(o.getName());
													if(sv == null)
													{
														logger.info("Modifying->Can't find the server "+o.getName()+" in cache");
														requestInfoLookupModify(o.getName(), new Integer(o.getValue()).toString());
													}
													else
													{
														sChannel = connectToOther(sv.ip, sv.port);
														String message = "DM"+o.getName()+" "+o.getValue()+"\0";
														DBWrite(message, sChannel);
														logger.info("DB wrote request to MODIFY to other DB ");
													}
												}
												
											}
											
											String message = "ok";
											DBWrite(message, dbo.sChannel);
											dbo.sChannel.close();
											synchronized(CP)
											{
												CP.remove(dbo);
											}
										}
										contor = i;
									}
									else if((char)buf.get(contor+1) == 'R')
									{
										String TID_s = new String("");
										int i = contor+2;
										while(true)
										{
										 char c = (char)buf.get(i++);
										 if(c == '\0')
											 break;
										 TID_s = TID_s + c;
										}										
										Data dbo;
										if((dbo=CP.get(TID_s)) == null)
										{
											logger.info("Transmission error(CL-R), the TID required does not exist!");
										}
										else
										{
											String message = "ok";
											DBWrite(message, dbo.sChannel);
											dbo.sChannel.close();
											synchronized(CP)
											{
												CP.remove(dbo);
											}
										}
										contor = i;
									}
									else
										logger.info("OOPS! Read something else from client");
								}
								else
									logger.info("OOPS! Read something else");
							}
						}
					}
				}
				
				if (selKey.isValid() && selKey.isWritable()) 
				{
					// Get channel that's ready for more bytes
					SocketChannel sChannel = (SocketChannel)selKey.channel();
				}

				if (selKey.isValid() && selKey.isAcceptable()) 
				{
					// Get channel that's ready for more bytes
					ServerSocketChannel sssChannel = (ServerSocketChannel)selKey.channel();
					
					SocketChannel sChannel = sssChannel.accept();
					ByteBuffer buf = ByteBuffer.allocateDirect(1024);
										
					if(sChannel != null)
					{
						logger.info("DB->Acceptable");
					    Socket socket = sChannel.socket();
					    sChannel.configureBlocking(false);
					    
					    // Register the new SocketChannel with our Selector, indicating
					    // we'd like to be notified when there's data waiting to be read
					    synchronized(sinc)
						{
							this.selector.wakeup();
					    	sChannel.register(this.selector, SelectionKey.OP_READ);
					    }
					    logger.info("DB: Done accepting");
					}
				}
		}
		catch(Exception ex)
		{
			
		}
	   }
	   
	   public void DBWrite(String message, SocketChannel sChannel)
	   {
		   	ByteBuffer buf2 = ByteBuffer.allocateDirect(1024);
		   	buf2.clear();						
			try {			    	
				buf2.put(message.getBytes());
				// Prepare the buffer for reading by the socket
				buf2.flip();
			
				int numToWrite = message.getBytes().length, wr = 0;
				while(wr<numToWrite)
				{
					int numBytesWritten = sChannel.write(buf2);
					wr+=numBytesWritten;
				}
		    } catch (IOException e) {
		    	logger.info("DB->Client closed connection");
			}
	   }
	 }
}