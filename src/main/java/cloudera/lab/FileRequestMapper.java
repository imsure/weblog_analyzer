package cloudera.lab;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FileRequestMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	private LogRecordParser record_parser = new LogRecordParser();

	enum ImageCounter {
		GIF,
		JPEG,
		OTHER
	}
	
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String line = value.toString();
		record_parser.feedInput(line);
		String filename = record_parser.getRequestedFileName();

		if (filename != null) {
			if (filename.endsWith("gif")) {
				context.getCounter(ImageCounter.GIF).increment(1);
			} else if (filename.endsWith("jpg")) {
				context.getCounter(ImageCounter.JPEG).increment(1);
			} else {
				context.getCounter(ImageCounter.OTHER).increment(1);
			}
		}
	}
}
