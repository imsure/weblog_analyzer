package cloudera.lab;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LogFileMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	private Text IPAddr = new Text();
	private IntWritable numHits = new IntWritable();
	private Map<String, Integer> hit_count;
	private LogRecordParser record_parser = new LogRecordParser();

	@Override
	public void setup(Context context)
			throws IOException, InterruptedException {
		hit_count = new HashMap<String, Integer>();
	}

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String line = value.toString();
		record_parser.feedInput(line);
		String ip_addr = record_parser.extractIPAddress();

		if (ip_addr != null) {
			if (hit_count.containsKey(ip_addr)) {
				hit_count.put(ip_addr, hit_count.get(ip_addr) + 1);
			} else {
				hit_count.put(ip_addr, 1);
			}
		}
	}

	@Override
	public void cleanup(Context context) 
			throws IOException, InterruptedException {
		for (String ip_addr : hit_count.keySet()) {
			IPAddr.set(ip_addr);
			numHits.set(hit_count.get(ip_addr));
			context.write(IPAddr, numHits);
		}
	}
}
