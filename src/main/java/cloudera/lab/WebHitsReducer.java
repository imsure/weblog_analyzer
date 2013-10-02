package cloudera.lab;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WebHitsReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	private IntWritable numHits = new IntWritable();
	
	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
	
		int hits_sum = 0;
		for (IntWritable value : values) {
			hits_sum += value.get();
		}
		numHits.set(hits_sum);
		context.write(key, numHits);
	}
}
