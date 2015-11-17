package insert2pg;

import java.io.FileInputStream;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.sql.*;



public class PgClient {

	Properties prop = new Properties();
	private long threadNum = 1;
	private long taskLoad = 10000;
	private long lowIndex = 0;
	private String user = "";
	private String password = "";
	private String server = "127.0.0.1";
	
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
		
		
		if(prop.containsKey("USER")){  
	        this.user = prop.getProperty("USER"); 
	    }
		if(prop.containsKey("PASSWORD")){  
	        this.password = prop.getProperty("PASSWORD"); 
	    }
		
		if(prop.containsKey("SERVER")){  
	        this.server = prop.getProperty("SERVER"); 
	    }
		
	}
	
	public static class Handler implements Runnable {
		   private long low;
		   private long high;
		   private String user;
		   private String psd;
		   private String url;
		   
		   Handler(long l, long h, String user, String pass, String addr) {
			   this.low = l; 
			   this.high = h;
			   this.url = "jdbc:postgresql://"+ addr + ":5432/gptest";	
			   this.user = user;
			   this.psd = pass;
		   }
		   
		   public void run() {
		     // read and service request on socket
			   Connection conn = null;
			   Statement st = null;
		       try {
		           Class.forName("org.postgresql.Driver");
		           conn = DriverManager.getConnection(url, user, psd);
		           st = conn.createStatement();
		       } catch (Exception e) {
		           e.printStackTrace();
		       }
		       		       
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
				   if (p%1000000 == 0) {
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
					   String sql = "INSERT INTO sanguo (id, name, country, url, ip) VALUES(" + 
							   Long.toString(i) + ", '" + name + "','"+ country + "','www.sanguo.com','" + ip + "')";
			           st.executeUpdate(sql);
			           //rs.close();			           			           
				   } catch (Exception e) { 
					   e.printStackTrace();
					   System.exit(-1);
				   }
			   }
			   
			   long endTime=System.nanoTime(); //获取结束时间
			   System.out.print("Take " + (endTime - startTime) + " nanos\n");
			   
			   try {
				   st.close();
		           conn.close();
			   } catch (Exception e) {
				   e.printStackTrace();
			   }
			   
		   }
	}
	
	
	public static void main(String[] args) {
		
		PgClient cli = new PgClient();
		cli.initConfig();
		// TODO Auto-generated method stub
 
		
		ExecutorService pool = Executors.newFixedThreadPool((int)cli.threadNum);
	    for(int i = 0;i < cli.threadNum ;i++){	    	
	    	pool.execute(new Handler(i * cli.taskLoad + cli.lowIndex, 
	    			(i + 1) * cli.taskLoad + cli.lowIndex, cli.user, cli.password,cli.server));
	    }
	    	    
	    pool.shutdown();
	}


}
