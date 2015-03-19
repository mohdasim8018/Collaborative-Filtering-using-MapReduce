package job2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class CFReducer1 extends MapReduceBase implements Reducer<Text, Text, Text,
Text>{

	public void reduce(Text key, Iterator<Text> values,
			OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
		
		ArrayList<Double> itemI = new ArrayList<Double>();
		ArrayList<Double> itemJ = new ArrayList<Double>();
		
		while  (values.hasNext()){
			String[] iJ = values.next().toString().split(",");
			itemI.add(Double.parseDouble(iJ[0]));
			itemJ.add(Double.parseDouble(iJ[1]));
		}
		
		double iSum = 0;
		double jSum = 0;
		
		for(int i=0; i<itemI.size(); i++){
			iSum= iSum+itemI.get(i);
			jSum = jSum+itemJ.get(i);
		}
		
		/*
		 * Mean of all users rating for a movie pair (i, j)
		 */
		double iMean = iSum/itemI.size();
		double jMean = jSum/itemJ.size();
		//System.out.println("I mean:"+iMean+","+"J mean"+jMean);
		double numerator = 0.0;
		double denominator = 0.0;
		double similarity = 0.0;
		/*
		 * Calculating numerator of Cosine similarity formula
		 */
		for (int i=0; i<itemI.size(); i++){
			numerator= numerator+(itemI.get(i) * itemJ.get(i));  
		}
		
		/*
		 * Calculating denominator of Cosine similarity formula
		 */
		double denom1 = 0.0;
		double denom2 = 0.0;
		for (int i=0; i<itemI.size(); i++){
			double part1 = (itemI.get(i))*(itemI.get(i));
			double part2 = (itemJ.get(i))*(itemJ.get(i));
			denom1 = denom1 + part1;
			denom2 = denom2 + part2;
		}
		denominator = Math.sqrt(denom1) * Math.sqrt(denom2);
		
		similarity = numerator/denominator;
		
		output.collect(key, new Text(Double.toString(similarity)));
	}

}
