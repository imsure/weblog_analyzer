package cloudera.lab;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import org.junit.Before;
import org.junit.Test;

import cloudera.lab.FileRequestMapper.ImageCounter;

import java.util.*;

public class WebHitsTest {

	MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
	ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;
	MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;

	@Before
	public void setUp() {
		FileRequestMapper mapper = new FileRequestMapper();

		mapDriver = new MapDriver<LongWritable, Text, Text, IntWritable>();
		mapDriver.setMapper(mapper);
		
		mapReduceDriver = new MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable>();
		mapReduceDriver.setMapper(mapper);
	}

	@Test
	public void testCounter() {
		Text record1 = new Text("10.153.239.5 - - [29/Jul/2009:09:25:40 -0700] \"GET " +
				"/release-schedule/logo.gif HTTP/1.1\" 200 9903");
		//mapDriver.withInput(new LongWritable(1), record1);

		Text record2 = new Text("10.153.239.5 - - [29/Jul/2009:09:25:40 -0700] " +
				"\"GET /assets/js/lowpro.js HTTP/1.1\" 304 -");
		mapDriver.withInput(new LongWritable(2), record2);
		
		Text record3 = new Text("10.223.157.186 - - [15/Jul/2009:14:58:59 -0700] \"GET /tag.gif HTTP/1.1\" 403 202");
		//mapDriver.withInput(new LongWritable(3), record3);
		
		mapDriver.runTest();
		
		assertEquals("Expected 0 gif counter increment.", 0, 
				mapDriver.getCounters().findCounter(ImageCounter.GIF).getValue());
		assertEquals("Expected 0 jpeg counter increment.", 0, 
				mapDriver.getCounters().findCounter(ImageCounter.JPEG).getValue());
		assertEquals("Expected 1 other counter increment.", 1, 
				mapDriver.getCounters().findCounter(ImageCounter.OTHER).getValue());
	}
}
