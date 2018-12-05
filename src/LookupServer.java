
import java.nio.*;
import java.nio.channels.*;
import java.nio.channels.spi.SelectorProvider;
import java.net.*;
import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import org.apache.log4j.Logger;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.FileAppender;
import org.apache.log4j.*;

class Server{
	String ip;
	int port;
	public Server(String ip, int port)
	{
		this.ip = ip;
		this.port = port;
	}
}

public class LookupServer extends Thread implements LookupServerInterface{

	String ip;
	int port;
	int poolSize;
	private ServerSocketChannel ssChannel;
	private Selector selector = null;
	private ExecutorService pool;
	HashMap<String, Server> DB;
	Object sinc = new Object();
	File f;
	FileAppender fapp;
	Logger logger;
	
	public void startServer(String myIP, int myPort)
	{
		poolSize = 4;
		ip=myIP;
		port=myPort;
		DB = new HashMap<String, Server>();
		
		try
		{
			logger = Logger.getLogger(LookupServer.class);
			f = new File("logs", "LookupServer"+myIP+myPort+".log");
			fapp = new FileAppender(new TTCCLayout("DATE"), f.getAbsolutePath());
			logger.addAppender(fapp);
			BasicConfigurator.configure();
			logger.setLevel((Level)Level.ALL);
			logger.info("Entering LookupServer");
		}
		catch(IOException e)
		{
			System.err.println("Unable to open logger");
		}
		
		try{
			pool = Executors.newFixedThreadPool(poolSize);
			
			//Create a new selector
			selector = SelectorProvider.provider().openSelector();

		    // Create a new non-blocking server socket channel
		    this.ssChannel = ServerSocketChannel.open();
		    ssChannel.configureBlocking(false);

		    // Bind the server socket to the specified address and port
		    InetSocketAddress isa = new InetSocketAddress(this.ip, this.port);
		    ssChannel.socket().bind(isa);

		    // Register the server socket channel, indicating an interest in accepting new connections
		    ssChannel.register(selector, SelectionKey.OP_ACCEPT);
		
			if (selector==null) logger.info("Selector null");
			else
			{
					//can be run
			}
		}
		catch(IOException e)
		{
			logger.info("Exception while starting LookupServer");
			e.printStackTrace();
		}
		this.start();
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
				logger.info("Lookup Server shutting down, no more tasks being submitted");
			}
			
		 try {
		     // Wait a while for existing tasks to terminate
		     if (!pool.awaitTermination(1, TimeUnit.SECONDS)) {
		       pool.shutdownNow(); // Cancel currently executing tasks
		       // Wait a while for tasks to respond to being cancelled
		       if (!pool.awaitTermination(1, TimeUnit.SECONDS))
		    	   logger.info("Pool did not terminate");
		     }
		     ssChannel.close();
		     selector.close();
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
		//Wait for events
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
					
				if(selector == null || (!selector.isOpen()))
					break;
				// Get list of selection keys with pending events
				Iterator it = selector.selectedKeys().iterator();
			
				// Process each key at a time
				while (it.hasNext()) 
				{
					// Get the selection key
					SelectionKey selKey = (SelectionKey)it.next();
				
					// Remove it from the list to indicate that it is being processed
					it.remove();
				
					pool.execute(new HandlerLookup(selKey, this.selector));
				}
			} 
			catch (IOException e) 
			{	// Handle error with selector
				logger.info("IO: Ending Lookup server");
				break;
			}
			catch(ClosedSelectorException e)
			{
				logger.info("Closed Selector: Ending Lookup server");
				break;
			}
			catch(RejectedExecutionException e)
			{
				logger.info("Thread Pool Unavailable : Ending Lookup server");
				break;
			}
			catch(Exception e)
			{
				logger.info("Ending Lookup server");
				break;
			}
		}
	}

class HandlerLookup implements Runnable {
	   private final SelectionKey selKey;
	   private Selector selector;
	   HandlerLookup(SelectionKey selKey, Selector selector) { this.selKey = selKey; this.selector = selector;}
	   
	   public void run() 
	   {
		 // read and service request on socket
		 try{
				if (selKey.isValid() && selKey.isConnectable())
				{
					logger.info("Lookup->connectable");
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
					buf.clear();
					
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
								if((char)buf.get(contor) == 'A')
								{
									logger.info("Adding new object: ");
									//Request for adding an object
									//Format: 'A'objectname{blank}IP{blank}PORT'\0'
									String n = new String(""); //name of the object to be added
									String portadd = new String("");//port of the server who has added it
									String ipadd = new String("");//ip of the server who has added it
									int i = contor+1;
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
										if(c == '\0')
											break;
										portadd = portadd + c;
									}
															
									DB.put(n, new Server(ipadd, Integer.parseInt(portadd)));
									contor = i;
									sChannel.close();
									selKey.cancel();
								}
								else if((char)buf.get(contor) == 'I')
								{
									logger.info("Lookup->Got info request from DB");
									//Info request about an object
									//Format:'I'objectname'\0'
									String n = new String("");
									String TID = new String("");
									int i = contor+1;
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
									contor = i;
									
									logger.info("Lookup->Info about "+n+" : ip "+DB.get(n).ip+" port "+DB.get(n).port);
									
									ByteBuffer buf2 = ByteBuffer.allocateDirect(1024);
									try 
									{
										buf2.clear();
										
										//Fill the buffer with the bytes to write;
										String message = "LA"+n+" "+DB.get(n).ip+" "+(new Integer(DB.get(n).port)).toString()+" "+TID+"\0";			
										buf2.put(message.getBytes());
										// Prepare the buffer for reading by the socket
										buf2.flip();
									
										// Write bytes
										int numToWrite = message.getBytes().length, wr = 0;
										while(wr<numToWrite)
										{
											int numBytesWritten = sChannel.write(buf2);
											wr+=numBytesWritten;
										}
										logger.info("Lookup wrote info requested "+wr);
									}
									catch (IOException e) {}
									
								}
								else if((char)buf.get(contor) == 'J')
								{
									logger.info("Lookup->Got info request (modify) from DB");
									String n = new String(""); //numele obiectului despre care aflu informatii
									String val = new String("");
									int i = contor+1;
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
									contor = i;
									
									logger.info("Lookup->Info about "+n+" : ip "+DB.get(n).ip+" port "+DB.get(n).port);
																		
									ByteBuffer buf2 = ByteBuffer.allocateDirect(1024);
									try 
									{
										buf2.clear();
										
										//Fill the buffer with the bytes to write;
										String message = "LB"+n+" "+DB.get(n).ip+" "+(new Integer(DB.get(n).port)).toString()+" "+val+"\0";			
										buf2.put(message.getBytes());
										// Prepare the buffer for reading by the socket
										buf2.flip();
									
										// Write bytes
										int numToWrite = message.getBytes().length, wr = 0;
										while(wr<numToWrite)
										{
											int numBytesWritten = sChannel.write(buf2);
											wr+=numBytesWritten;
										}
										logger.info("Lookup wrote info (modify) requested "+wr);
									}
									catch (IOException e) {}
									
								}
								else
									break;
							}
						}
						
					}
					
				}
				
				if (selKey.isValid() && selKey.isWritable()) 
				{
					logger.info("Lookup->Writable");
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
						logger.info("Lookup->Acceptable");
					    Socket socket = sChannel.socket();
					    sChannel.configureBlocking(false);
	
					    // Register the new SocketChannel with our Selector, indicating
					    // we'd like to be notified when there's data waiting to be read
					    synchronized(sinc)
					    {
					    	this.selector.wakeup();
					    	sChannel.register(this.selector, SelectionKey.OP_READ);
					    }
					    logger.info("Lookup: Done accepting");
					}
				}
		}
		catch(Exception ex)
		{
			
		}
	   }
	 }
}
