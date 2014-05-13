package nl.surfsara.warcexamples.hadoop.warc;

import nl.surfsara.warcutils.WarcSequenceFileInputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

public class Hrefs extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {

		Configuration conf = this.getConf();

		Job job = Job.getInstance(conf, "Extract hrefs");
		job.setJarByClass(Hrefs.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(HrefExtracter.class);
		job.setReducerClass(Reducer.class);
		job.setInputFormatClass(WarcSequenceFileInputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// Execute job and return status
		return job.waitForCompletion(true) ? 0 : 1;

	}

}
