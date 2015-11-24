package kafka_consumer;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.Properties;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.json.JSONException;  
import org.json.JSONObject;  


import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import sun.misc.Signal;
import sun.misc.SignalHandler;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import sun.misc.Signal;  
import sun.misc.SignalHandler; 



public class myconsumer implements SignalHandler {
    private ConsumerConnector consumer = null;
    private ExecutorService executor;
    private Properties prop = new Properties();
    public boolean stop = false;
    
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
		/*
		if(prop.containsKey("NUM_OF_THREAD")){  
	        this.threadNum = Integer.valueOf(prop.getProperty("NUM_OF_THREAD"));  
	    }
	    */
				
	}
 
	private void signalCallback(Signal sn) {  
        System.out.println(sn.getName() + "is recevied.");  
        shutdown();
        
    }  
   
    @Override  
    public void handle(Signal signalName) {  
        signalCallback(signalName);  
    }
    
    public class ConsumerThread implements Runnable {
        private KafkaStream m_stream;
        private String m_path;
        private int m_threadNumber;
        private myconsumer m_consumer;
        private SimpleDateFormat ts2f =new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
     
        public ConsumerThread(KafkaStream a_stream, int a_threadNumber, String path, myconsumer c) {
            m_threadNumber = a_threadNumber;
            m_path = path;
            m_stream = a_stream;
            m_consumer = c;
        }
        
        private boolean convertLine(String json, StringBuilder l){
        	try{
	        	JSONObject inputJSONObject = new JSONObject(json);
				if (!inputJSONObject.has("timestamp")){       						
					return false;
				}
				if (!inputJSONObject.has("cid")){
					return false;
				}
				if (!inputJSONObject.has("method")){
					return false;
				}
				
				l.delete(0, l.length());
				
				java.util.Date create_time = 
						new java.util.Date(Math.round(1000*Double.parseDouble(inputJSONObject.getString("timestamp"))));
				l.append(ts2f.format(create_time));
				l.append("|");
				l.append(inputJSONObject.getString("method"));
				l.append("|");
				l.append(inputJSONObject.getString("cid"));
				l.append("|");
				if (inputJSONObject.has("user_agent")){
					l.append(inputJSONObject.getString("user_agent"));
					l.append("|");
				}
				else
					l.append("nul|");
				
				l.append("nul|"); // item_id
				
				if (inputJSONObject.has("uid")){
					l.append(inputJSONObject.getString("uid"));
					l.append("|");
				}
				else
					l.append("nul|");
				if (inputJSONObject.has("gid")){
					l.append(inputJSONObject.getString("gid"));
					l.append("|");
				}
				else
					l.append("nul|");
				if (inputJSONObject.has("uuid")){
					l.append(inputJSONObject.getString("uuid"));
					l.append("|");
				}
				else
					l.append("nul|");
				
				if (inputJSONObject.has("sid")){
					l.append(inputJSONObject.getString("sid"));
					l.append("|");
				}
				else
					l.append("nul|");
				
				if (inputJSONObject.has("price")){
					l.append(inputJSONObject.getString("price"));
					l.append("|");
				}
				else
					l.append("nul|");
				
				l.append("nul|"); // guantity
				l.append("nul|"); // order_id
				
				if (inputJSONObject.has("ip")){
					l.append(inputJSONObject.getString("ip"));
					l.append("|");
				}
				else
					l.append("nul|");
				
				if (inputJSONObject.has("ref_page")){
					l.append(inputJSONObject.getString("ref_page"));
					l.append("|");
				}
				else
					l.append("nul|");
				
				//insert_time					
				l.append(ts2f.format(new java.util.Date()));
				l.append("|");
				
				if (inputJSONObject.has("tma")){
					l.append(inputJSONObject.getString("tma"));
					l.append("|");
				}
				else
					l.append("nul|");
				if (inputJSONObject.has("tmd")){
					l.append(inputJSONObject.getString("tmd"));
					l.append("|");
				}
				else
					l.append("nul|");
				if (inputJSONObject.has("tmc")){
					l.append(inputJSONObject.getString("tmc"));
					l.append("|");
				}
				else
					l.append("nul|");
				
				l.append("nul|"); // entry_page
				l.append("nul|"); // link
				l.append("nul|"); // link_keyword
				l.append("nul|"); // code_type
				
				if (inputJSONObject.has("p_t")){
					l.append(inputJSONObject.getString("p_t"));
					l.append("|");
				}
				else
					l.append("nul|");
				
				l.append("nul|"); // signature
				l.append("nul|"); // parent
				l.append("nul|"); // browser_type
				l.append("nul|"); // os_type
				l.append("nul|"); // os_code
				l.append("nul|"); // resolution
				l.append("nul|"); // username
				l.append("nul|"); // flash_version       					
				if (inputJSONObject.has("ja")){
					l.append(inputJSONObject.getString("ja"));
					l.append("|");
				}
				else
					l.append("nul|");
				l.append("nul|"); // color_bit
				l.append("nul|"); // loadtime_leftpix
				l.append("nul|"); // score
				l.append("nul|"); // content
				l.append("nul|"); // total
				l.append("nul|"); // currency
				l.append("nul|"); // address
				l.append("nul|"); // express
				l.append("nul|"); // pay
				l.append("nul|"); // mobile
				l.append("nul|"); // name
				l.append("nul|"); // express_price
				
				if (inputJSONObject.has("qstr")){
					l.append(inputJSONObject.getString("qstr"));
					l.append("|");
				}
				else
					l.append("nul|");
				if (inputJSONObject.has("emp")){
					l.append(inputJSONObject.getString("emp"));
					l.append("|");
				}
				else
					l.append("nul|");
				
				// from rid to s_addr
				for (int i = 0; i < 42; ++i )
					l.append("nul|");
				l.append("nul\n");
	        } catch (JSONException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
        	
        	return true;
        }
            
        public void run() {
            ConsumerIterator<byte[], byte[]> it = m_stream.iterator();
            
            String  current_file;
            java.util.Date date=new java.util.Date();
    		SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd-HH");      		  
    		current_file = sdf.format(date);
    		FileOutputStream outStream = null;
    		
    		try {
				outStream = new FileOutputStream(m_path + current_file);
			} catch (FileNotFoundException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
            
            while (!m_consumer.stop) {
            	date=new java.util.Date();        		  
        		String fn=sdf.format(date);
        		if(0 != fn.compareTo(current_file)){
        			try {
						outStream.close();
						current_file = fn;
						outStream = new FileOutputStream(m_path + current_file);
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
        		}
        		
        		StringBuilder l = new StringBuilder(1024);
        		
        		
        		if (it.hasNext()){
        			
        			String line = new String(it.next().message());

        			if (line.startsWith("{DS.Input.")){
        				String json= line.substring(line.indexOf('}') + 1);
        				if (!convertLine(json, l))
        					continue;
        				try {      					
        					outStream.write(l.toString().getBytes());     					
        				} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						} 
        			}
        			else{
        				continue;
        			}
        			
        		}
        		else{
        			try {
        				System.out.println("has not Next");
        	            Thread.sleep(10000);
        	        } catch (InterruptedException e) {
        	            e.printStackTrace(); 
        	        }        			
        		}
        		                
            }
            
            try {
				outStream.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
            //System.out.println("Shutting down Thread: " + m_threadNumber);
        }
    }
    
    public void shutdown() {

        if (consumer != null)
            consumer.shutdown();
        stop = true;
        if (executor != null)
            executor.shutdown();
    }
    
    public void run(String dirs) {
    	String [] out_paths = dirs.split(",");
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        String topic = prop.getProperty("topic");
        
        
        topicCountMap.put(topic, new Integer(out_paths.length));
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
        List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);
 
        // now launch all the threads
        //
        executor = Executors.newFixedThreadPool(out_paths.length);
 
        // now create an object to consume the messages
        //
        int threadNumber = 0;
        for (final KafkaStream stream : streams) {
            executor.submit(new ConsumerThread(stream, threadNumber, out_paths[threadNumber],this));
            threadNumber++;
        }
    }
 
    public static void main(String[] arg) {
    	 
        myconsumer c = new myconsumer();
        c.initConfig();
        
        Signal.handle(new Signal("INT"), c);
        
        String dirs = c.prop.getProperty("output_path");
               
        c.consumer = kafka.consumer.Consumer.createJavaConsumerConnector(new ConsumerConfig(c.prop));
               
        c.run(dirs);
 
        
        while (!c.stop)
        try {
            Thread.sleep(30000);
        } catch (InterruptedException ie) {
 
        }
      
    }
}
