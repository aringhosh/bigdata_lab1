import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class EulerEstimator extends Configured implements Tool {

	public static class TokenizerMapper
	extends Mapper<LongWritable, Text, Text, IntWritable>{

		@Override
		public void map(LongWritable key, Text value, Context context
				) throws IOException, InterruptedException {

			String fileName = ((org.apache.hadoop.mapred.FileSplit) context.getInputSplit()).getPath().getName();
			java.util.Random r = new  java.util.Random(fileName.hashCode());
			long iterations = Integer.parseInt(value.toString());
			double sum = 0.0;
			long count = 0;

			for(long i =0; i < iterations; i++) {
				while(sum < 1.0) {
					sum += r.nextDouble();
					count++;
				}
			}
			
			context.getCounter("Euler", "iterations").increment(iterations);
			context.getCounter("Euler", "count").increment(count);
		}
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new EulerEstimator(), args);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = this.getConf();
		Job job = Job.getInstance(conf, "Euler Estimation");
		job.setJarByClass(EulerEstimator.class);

		job.setInputFormatClass(TextInputFormat.class);

		job.setMapperClass(TokenizerMapper.class);
//		job.setCombinerClass(IntSumReducer.class);
//		job.setReducerClass(IntSumReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setOutputFormatClass(NullOutputFormat.class);
		TextInputFormat.addInputPath(job, new Path(args[0]));
		//NullOutputFormat.setOutputPath(job, new Path(args[1]));
		
		return job.waitForCompletion(true) ? 0 : 1;
	}
}