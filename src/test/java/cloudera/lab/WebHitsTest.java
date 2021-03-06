package cloudera.lab;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import static org.junit.Assert.fail;
import org.junit.Before;
import org.junit.Test;
import java.util.*;

public class WebHitsTest {

	MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
	ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;
	MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;

	@Before
	public void setUp() {
		LogFileMapper mapper = new LogFileMapper();
		WebHitsReducer reducer = new WebHitsReducer();

		mapDriver = new MapDriver<LongWritable, Text, Text, IntWritable>();
		mapDriver.setMapper(mapper);
		reduceDriver = new ReduceDriver<Text, IntWritable, Text, IntWritable>();
		reduceDriver.setReducer(reducer);
		mapReduceDriver = new MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable>();
		mapReduceDriver.setMapper(mapper);
		mapReduceDriver.setReducer(reducer);
	}

	@Test
	public void testMapper() {
		
		Text record1 = new Text("10.223.157.186 - - [15/Jul/2009:14:58:59 -0700] \"GET / HTTP/1.1\" 403 202\n");		
		Text record2 = new Text("10.223.157.111 - - [15/Sep/2009:14:58:59 -0700] \"GET / HTTP/1.1\" 403 202\n");
		
		mapDriver
		.withInput(new LongWritable(1), record1)
		.withInput(new LongWritable(2), record2);

		mapDriver
		.withOutput(new Text("10.223.157.111	Sep"), new IntWritable(1))
		.withOutput(new Text("10.223.157.186	Jul"), new IntWritable(1));
		
		mapDriver.runTest();
	}
	
	@Test
	public void testMR() {
		Text record1 = new Text("10.153.239.5 - - [29/Jul/2009:09:25:40 -0700] \"GET " +
				"/release-schedule/ HTTP/1.1\" 200 9903");
		mapReduceDriver.withInput(new LongWritable(1), record1);

		Text record2 = new Text("10.153.239.5 - - [29/Jul/2009:09:25:40 -0700] " +
				"\"GET /assets/js/lowpro.js HTTP/1.1\" 304 -");
		mapReduceDriver.withInput(new LongWritable(2), record2);
		
		Text record3 = new Text("10.223.157.186 - - [15/Jul/2009:14:58:59 -0700] \"GET / HTTP/1.1\" 403 202");
		mapReduceDriver.withInput(new LongWritable(3), record3);

		mapReduceDriver.withOutput(new Text("10.153.239.5	Jul"), new IntWritable(2));
		mapReduceDriver.withOutput(new Text("10.223.157.186	Jul"), new IntWritable(1));
		
		mapReduceDriver.runTest();
	}
}
