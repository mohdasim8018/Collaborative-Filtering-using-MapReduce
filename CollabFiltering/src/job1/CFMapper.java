package job1;
import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class CFMapper extends MapReduceBase
implements Mapper<LongWritable, Text, Text,Text>{

	public void map(LongWritable key, Text value,
			OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
		
		String[] values = value.toString().split("\t");
		String valueFinal = values[1]+","+values[2];
		output.collect(new Text(values[0]), new Text(valueFinal));
	}

}
