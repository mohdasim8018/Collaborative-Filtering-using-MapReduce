package job2;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class CFMapper1 extends MapReduceBase
implements Mapper<LongWritable, Text, Text,Text>{

	public void map(LongWritable key, Text value,
			OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
		
		String[] values = value.toString().split("\t");
		
		for (int i = 1; i < values.length-1; i++){
			for (int j = i+1; j< values.length; j++){
				String[] cur = values[i].split(",");
				String[] next = values[j].split(",");
				String keyFinal = cur[0]+","+next[0];
				String valueFinal = cur[1]+","+next[1];
				output.collect(new Text(keyFinal), new Text(valueFinal));
			}
		}
		
		
		
	}

}
