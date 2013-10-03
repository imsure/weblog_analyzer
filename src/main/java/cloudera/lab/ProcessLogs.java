package cloudera.lab;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import cloudera.lab.FileRequestMapper.ImageCounter;

public class ProcessLogs extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {

		if (args.length != 1) {
			System.err.printf("Usage: %s [generic options] [input path]\n",
					getClass().getSimpleName());
			ToolRunner.printGenericCommandUsage(System.err);
			System.exit(-1);
		}

		Job job = new Job(getConf());
		job.setJarByClass(ProcessLogs.class);
		job.setJobName("Web Resources Counter");
		job.setMapperClass(FileRequestMapper.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setNumReduceTasks(0);

		// No output needed for the job.
		job.setOutputFormatClass(NullOutputFormat.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		
		boolean success = job.waitForCompletion(true);

		if (success) {
			long gif_count = job.getCounters().findCounter(ImageCounter.GIF).getValue();
			long jpg_count = job.getCounters().findCounter(ImageCounter.JPEG).getValue();
			long other_count = job.getCounters().findCounter(ImageCounter.OTHER).getValue();

			System.out.println("JPG   = " + jpg_count);
			System.out.println("GIF   = " + gif_count);
			System.out.println("OTHER = " + other_count);
			return 0;
		} else {
			return 1;
		}
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new ProcessLogs(), args);
		System.exit(res);
	}
}
