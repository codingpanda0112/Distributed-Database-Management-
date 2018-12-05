
import java.io.*;
import java.nio.channels.*;
import java.nio.*;
import java.net.*;
import java.util.*;
import org.apache.log4j.Logger;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.FileAppender;
import org.apache.log4j.*;

public class Client implements TransactionInterface{

	String ip;
	SocketChannel sChannel;
	String TID;
	HashMap<String, Integer> obiecte;
	File f;
	FileAppender fapp;
	Logger logger;
	
	Client()
	{
		super();
		TID = null;
		obiecte = new HashMap<String, Integer>();
	}
	
	Client(String ip)
	{
		this();
		this.ip = ip;
	}
	
	//connect to Database server
	public String begin(String DatabaseServerIP, int DatabaseServerPort)
	{
			// Create a non-blocking socket and check for connections
			try {
				// Create a non-blocking socket channel on port 80
				sChannel = createSocketChannel(DatabaseServerIP, DatabaseServerPort);
	
				// Before the socket is usable, the connection must be completed
				//by calling finishConnect(), which is non-blocking
				while (!sChannel.finishConnect()) {
					//Do something else...waste time
				}
				// Socket channel is now ready to use
				//logger.info("Client->Connected.");
				
				String message = "CC\0";
				clWrite(message);
				//logger.info("Client wrote request for TID");
				
				ByteBuffer buf = ByteBuffer.allocateDirect(1024);
				buf.clear();
				int numBytesRead;
			    try {					
					while((numBytesRead = sChannel.read(buf))<1) ;
					
					//logger.info("Client->TID->Am citit " + numBytesRead);
					if (numBytesRead == -1) 
					{
						sChannel.close();
					} 
					else 
					{
						buf.flip();
						// Read the bytes from the buffer ...;// see e159 Getting Bytes from a ByteBuffer
						if(numBytesRead>0)
						{
							int contor = 0;
							while(contor < numBytesRead)
							{
								if((char)buf.get(contor) == 'C')
								{
									//logger.info("Client->Got DB answer - TID");
									TID = new String("");//TIDul asociat de DB server clientului
									int i = contor+1;
									while(true)
									{
										char c = (char)buf.get(i++);
										if(c == '\0')
											break;
										TID = TID + c;
									}
									contor = i;
									
									try
									{
										logger = Logger.getLogger(Client.class);
										f = new File("logs" , "Client"+TID+".log");
										fapp = new FileAppender(new TTCCLayout("DATE"), f.getAbsolutePath());
										logger.addAppender(fapp);
										BasicConfigurator.configure();
										logger.setLevel((Level)Level.ALL);
										logger.info("Entering Client");
										logger.info("Client->Connected.");
										logger.info("Client wrote request for TID");
										logger.info("Client->TID->Am citit " + numBytesRead);
										logger.info("Client->Got DB answer - TID");
									}
									catch(IOException e)
									{
										System.err.println("Unable to open logger");
									}
									logger.info("Client received TID: "+TID);
								}
							}
						}
					}
					
				} catch (IOException e) {
					logger.info("Client->Server closed connection");
				}
			}
			catch (IOException e) {
				logger.info("Exception while begining Client");
				e.printStackTrace();
			}
			return TID;
	}
	public int getValue(String objName)
	{
		Integer ii;
		if((ii = obiecte.get(objName)) != null)
		{
			return ii.intValue();
		}
		
		ByteBuffer buf = ByteBuffer.allocateDirect(1024);
		buf.clear();
		String message = "CG"+TID+" "+objName+"\0";
		clWrite(message);
		logger.info("Client wrote request for value of "+objName);
		String n = new String("");
		int numBytesRead;
	    try {			
			while((numBytesRead = sChannel.read(buf))<1) ;
			
			logger.info("Client->Value->Read" + numBytesRead);
			if (numBytesRead == -1) 
			{
				sChannel.close();
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
							logger.info("Client->Got DB answer - Value");
							int i = contor+1;
							while(true)
							{
								char c = (char)buf.get(i++);
								if(c == '\0')
									break;
								n = n + c;
							}
							contor = i;
							logger.info("Client received value: "+n);
							obiecte.put(objName, Integer.parseInt(n));
							return Integer.parseInt(n);
						}
					}
				}
			}
			
		} catch (IOException e) {
			logger.info("Client->Server closed connection");
		}
		return -1;
	}
	public void setValue(String objName, int value)
	{
		String message = "CS"+TID+" "+objName+" "+value+"\0";
		clWrite(message);
		obiecte.put(objName, value);
		logger.info("Client wrote request to set the value of "+objName);
	}
	public void commit()
	{
		String message = "CM"+TID+"\0";
		clWrite(message);
		int numBytesRead;
		ByteBuffer buf = ByteBuffer.allocateDirect(1024);
		buf.clear();
		try
		{
			while((numBytesRead = sChannel.read(buf))<1);
			sChannel.close();
		}catch (IOException e) 
		{
			logger.info("Client->Server closed connection while commiting");
		}
	}
	
	public void rollback()
	{
		String message = "CR"+TID+"\0";
		clWrite(message);
		int numBytesRead;
		ByteBuffer buf = ByteBuffer.allocateDirect(1024);
		buf.clear();
		try
		{
			while((numBytesRead = sChannel.read(buf))<1);
			sChannel.close();
		}catch (IOException e) 
		{
			logger.info("Client->Server closed connection at rollback");
		}
	}
	
	public void clWrite(String message)
	{
		ByteBuffer buf = ByteBuffer.allocateDirect(1024);
		int numBytesRead;
	    try {			    	
			buf.put(message.getBytes());
			// Prepare the buffer for reading by the socket
			buf.flip();
		
			int numToWrite = message.getBytes().length, wr = 0;
			while(wr<numToWrite)
			{
				int numBytesWritten = sChannel.write(buf);
				wr+=numBytesWritten;
			}
	    } catch (IOException e) {
	    	logger.info("Client->Server closed connection");
		}
	}
	
		
	public  SocketChannel createSocketChannel(String hostName, int port) throws IOException {
        // Create a non-blocking socket channel
	SocketChannel sChannel = SocketChannel.open();
	sChannel.configureBlocking(false);
    
        // Send a connection request to the server; this method is non-blocking
		sChannel.connect(new InetSocketAddress(hostName, port));
		return sChannel;
	}
}
