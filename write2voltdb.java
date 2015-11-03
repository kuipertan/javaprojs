package insert2vdb;

//import java.io.BufferedInputStream;
import java.io.FileInputStream;
//import java.io.FileNotFoundException;
import java.util.Properties; 
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
//import org.voltdb.*;
import org.voltdb.client.*;
import java.util.Random;

public class VdbClient {

	Properties prop = new Properties();
	private long threadNum = 1;
	private long taskLoad = 10000;
	private long lowIndex = 0;
	private String user = "";
	private String password = "";
	private String[] servers;
	
	void initConfig(){
		FileInputStream in;
		try {
			in = new FileInputStream("cli.properties");
			prop.load(in);
			in.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		if(prop.containsKey("NUM_OF_THREAD")){  
	        this.threadNum = Integer.valueOf(prop.getProperty("NUM_OF_THREAD"));  
	    }
		
		if(prop.containsKey("TASK_PER_THREAD")){  
	        this.taskLoad = Integer.valueOf(prop.getProperty("TASK_PER_THREAD"));  
	    }
		
		if(prop.containsKey("MIN_INDEX")){  
	        this.lowIndex = Integer.valueOf(prop.getProperty("MIN_INDEX"));  
	    }
		
		if(prop.containsKey("SERVERS")){  
	        this.servers = prop.getProperty("SERVERS").split(","); 
	    }
		
		if(prop.containsKey("USER")){  
	        this.user = prop.getProperty("USER"); 
	    }
		if(prop.containsKey("PASSWORD")){  
	        this.password = prop.getProperty("PASSWORD"); 
	    }
		
	}
	
	public static class Handler implements Runnable {
		   private long low;
		   private long high;
		   Client client = null;
		   Handler(long l, long h, String user, String pass, String[] addrs) {
			   this.low = l; 
			   this.high = h;
			   				
				ClientConfig config = new ClientConfig(user, pass);
				config.setProcedureCallTimeout(90 * 1000);
				config.setConnectionResponseTimeout(180 * 1000);
				config.setReconnectOnConnectionLoss(true);
				try {
					client = ClientFactory.createClient(config);
					for (int i = 0; i < addrs.length; ++i){
						client.createConnection(addrs[i]);
					}
				} catch (java.io.IOException e) {
					e.printStackTrace();
					System.exit(-1);
				}
		   }
		   
		   public void run() {
		     // read and service request on socket
			   ClientResponse resp;
			   Random random = new Random();
			   int ip0,ip1,ip2,ip3;
			   String url = "http://www.sanguo.com";
			   String []countrys = {"dongwu", "bashu", "beiwei","donghan","xijin"};
			   String []names = {"zhangfei", "liubei", "caomengde","daqiao",
					   "simayan","simayi", "diaochan","xuchu","lvbu","guanyu",
					   "zhaoyun","sunquan","zhouyu","weiyan", "lusu","huangzhong"};

			   int p = 0;
			   Thread current = Thread.currentThread();  
			   long tid = current.getId();
			   
			   long startTime=System.nanoTime();   //获取开始时间			   
			 
			   for (long i = low;  i < high; ++i){
				   ++p;
				   if (p%100000 == 0) {
					   System.out.println("Thread " + tid + " is inserting " + i);
					   p = 0;
				   }
				   ip0 = Math.abs(random.nextInt()%253) + 1;
				   ip1 = Math.abs(random.nextInt()%254) + 1;
				   ip2 = Math.abs(random.nextInt()%254) + 1;
				   ip3 = Math.abs(random.nextInt()%254) + 1;
				   String ip = Integer.toString(ip0) + "." + 
						   Integer.toString(ip1) + "." + 
						   Integer.toString(ip2) + "." + 
						   Integer.toString(ip3);
				   String name = names[ip3 % names.length] + Integer.toString((int)i);
				   String country = countrys[ip2 % countrys.length];
				   
				   
				   try { 
					   resp = client.callProcedure("sanguo.insert", 
							   						i,name,country,url,ip);
					   byte stat = resp.getStatus();
					   if (stat != ClientResponse.SUCCESS){
						   if (stat == ClientResponse.CONNECTION_TIMEOUT) {
							   	System.out.println("A procedure invocation has timed out.");
						   }
						   if (stat == ClientResponse.CONNECTION_LOST) {
							   System.out.println("Connection lost before procedure response.");
						   }
					   }
				   } catch (Exception e) { 
					   e.printStackTrace();
					   System.exit(-1);
				   }
			   }
			   long endTime=System.nanoTime(); //获取结束时间
			   System.out.print("Take " + (endTime - startTime) + " nanos\n");
			   try {
				   client.drain();
				   client.close();
			   } catch (Exception e) {
				   e.printStackTrace();
			   }
			   
		   }
	}
	
	
	public static void main(String[] args) {
		
		VdbClient cli = new VdbClient();
		cli.initConfig();
		// TODO Auto-generated method stub
 
		
		ExecutorService pool = Executors.newFixedThreadPool((int)cli.threadNum);
	    for(int i = 0;i < cli.threadNum ;i++){	    	
	    	pool.execute(new Handler(i * cli.taskLoad + cli.lowIndex, 
	    			(i + 1) * cli.taskLoad + cli.lowIndex, cli.user, cli.password,cli.servers));
	    }
	    	    
	    pool.shutdown();
	}

}
