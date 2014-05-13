package nl.surfsara.warcexamples.hadoop.wat;

import nl.surfsara.warcutils.WarcInputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;
import org.apache.hadoop.util.Tool;

public class ServerType extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {

		Configuration conf = this.getConf();

		Job job = Job.getInstance(conf, "Example Job");
		job.setJarByClass(ServerType.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(ServerTypeExtracter.class);
		job.setReducerClass(LongSumReducer.class);
		job.setInputFormatClass(WarcInputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);

		// Execute job and return status
		return job.waitForCompletion(true) ? 0 : 1;

	}
	
}
