package driver;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;

import job1.CFMapper;
import job1.CFReducer;
import job2.CFMapper1;
import job2.CFReducer1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;


public class CFDriver {
	
	public Set<String> getKeysByValue(Map<String, Double> map, double value) {
	    Set<String> keys = new HashSet<String>();
	    for (Map.Entry<String, Double> entry : map.entrySet()) {
	        if (value == entry.getValue()) {
	            keys.add(entry.getKey());
	        }
	    }
	    return keys;
	}

	public static void main(String[] args) throws IOException {
			if(args.length == 2){
				/*
				 * Read the centroid file into a Hashmap
				 */
				
				/*
				try{
					Path pt=new Path("/user/root/input/data");
			        FileSystem fs = FileSystem.get(new Configuration());
			        BufferedReader bufferReader = new BufferedReader(new InputStreamReader(fs.open(pt)));
			        String line;
		
			        while ((line = bufferReader.readLine()) != null)   {
			        	
			        }
			        bufferReader.close();
			    }
			    catch(Exception e){
			    	System.out.println("Error while reading file line by line:" + e.getMessage());                      
			    }*/
				
				/*
				 * 	Job 1: Gathering each user's list of rated movies
				 */
					JobClient client1 = new JobClient();
					JobConf conf1 = new JobConf(CFDriver.class);
					
					conf1.setMapperClass(CFMapper.class);
					//conf1.setPartitionerClass(CFPartitioner.class);
					conf1.setReducerClass(CFReducer.class);
					//conf1.setNumReduceTasks(3);
					
					//Set the output types for mapper and reducer Class
					conf1.setMapOutputKeyClass(Text.class);
					conf1.setMapOutputValueClass(Text.class);
					conf1.setOutputKeyClass(Text.class);
					conf1.setOutputValueClass(Text.class);
						
					String input, output;	
							
					input = args[0];	
						
					output = args[1];		
					FileInputFormat.setInputPaths(conf1, new Path(input));
					FileOutputFormat.setOutputPath(conf1, new Path(output));
					client1.setConf(conf1);
							
					try {
						JobClient.runJob(conf1);
					} 
					catch (Exception e) {
					e.printStackTrace();
					}
					
					
					JobClient client2 = new JobClient();
					JobConf conf2 = new JobConf(CFDriver.class);
					
					conf2.setMapperClass(CFMapper1.class);
					//conf1.setPartitionerClass(CFPartitioner.class);
					conf2.setReducerClass(CFReducer1.class);
					//conf1.setNumReduceTasks(3);
					
					//Set the output types for mapper and reducer Class
					conf2.setMapOutputKeyClass(Text.class);
					conf2.setMapOutputValueClass(Text.class);
					conf2.setOutputKeyClass(Text.class);
					conf2.setOutputValueClass(Text.class);
						
					String input1, output1;	
							
					input1 = "/user/root/output/part-00000";	
						
					output1 = args[1]+"1";		
					FileInputFormat.setInputPaths(conf2, new Path(input1));
					FileOutputFormat.setOutputPath(conf2, new Path(output1));
					client2.setConf(conf2);
							
					try {
						JobClient.runJob(conf2);
					} 
					catch (Exception e) {
					e.printStackTrace();
					}
					
					/*
					 * Sort the second job output file and sort for top 100 movie pairs
					 */
					try{
						Path pt=new Path("/user/root/output1/part-00000");
				        FileSystem fs = FileSystem.get(new Configuration());
				        BufferedReader bufferReader = new BufferedReader(new InputStreamReader(fs.open(pt)));
				        String line;
				        //HashMap<String, Double> similarity = new HashMap<String, Double>();
				        LinkedHashMap<String, Double> similarity = new LinkedHashMap<String, Double>();
			
				        while ((line = bufferReader.readLine()) != null)   {
				        	String[] temp = line.split("\t");
				        	double value = Double.parseDouble(temp[1]);
				        	similarity.put(temp[0], value);
				        }
				        bufferReader.close();
				        
				        LinkedHashSet<Double> values = new LinkedHashSet<Double>();
				        for (Map.Entry<String, Double> entry : similarity.entrySet()) {
				            values.add(entry.getValue());
				            System.out.println(entry.getKey()+": "+entry.getValue());
				        }
				        
				        ArrayList<Double> valueF = new ArrayList<Double>();
				        for(double temp: values){
				        	valueF.add(temp);
				        }
				        Collections.sort(valueF);
				        Collections.reverse(valueF);
				        //System.out.println("Sorted: ");
				        
				        int count = 0;
				        
				        Path pt2=new Path("/user/root/final/final.txt");
						FileSystem fs1 = FileSystem.get(new Configuration());
						FSDataOutputStream fsOutStream = fs1.create(pt2, true);
						BufferedWriter br1 = new BufferedWriter( new OutputStreamWriter(fsOutStream, "UTF-8" ) );
						//br.write("Hello World");
						//br1.write("C1	23357,401753,229671,826166,670144,946988,255137,89322,361894,828360"+"\n");
						//br1.close();
				        
				        CFDriver obj = new CFDriver();
				        for(double temp: valueF){
				        	Set<String> tempKeys = obj.getKeysByValue(similarity, temp);
				        	if(!tempKeys.isEmpty()){
				        		for(String temp1: tempKeys){
				        		if(count == 100) {
				        			br1.close();
				        			System.exit(0);
				        		}
				        		System.out.println("Final output- "+temp1+": "+temp );
				        		br1.write(temp1+"\t"+temp+"\n");
				        		count++;
				        		}
				        	}
				        	
				        }
				        
				    }
				    catch(Exception e){
				    	System.out.println("Error while reading file line by line:" + e.getMessage());                      
				    }
					
					/*
					Path pt2=new Path("/user/root/input1/centroid.txt");
					FileSystem fs = FileSystem.get(new Configuration());
					FSDataOutputStream fsOutStream = fs.create(pt2, true);
					BufferedWriter br = new BufferedWriter( new OutputStreamWriter(fsOutStream, "UTF-8" ) );
					//br.write("Hello World");
					br.write("C1	23357,401753,229671,826166,670144,946988,255137,89322,361894,828360"+"\n");
					br.close();*/
			}
			else
			{
				System.out.println("Invalid command line arguments!!!");
			}

	}

}
