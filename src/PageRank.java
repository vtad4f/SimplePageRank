import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class PageRank {

	public static class DoubleArrayWritable extends ArrayWritable { // https://stackoverflow.com/questions/28914596/mapreduce-output-arraywritable

		public DoubleArrayWritable() {
			super(DoubleWritable.class, new DoubleWritable[3]);
		}

		public DoubleArrayWritable(DoubleWritable[] values) {
			super(DoubleWritable.class, values);
		}

		@Override
		public DoubleWritable[] get() {
			Writable[] baseVals = super.get();
			DoubleWritable[] values = new DoubleWritable[3];
			values[0] = (DoubleWritable) baseVals[0];
			values[1] = (DoubleWritable) baseVals[1];
			values[2] = (DoubleWritable) baseVals[2];
			return values;
		}

		@Override
		public String toString() {
			DoubleWritable[] values = get();
			return String.format(
				"%10.2f %10.2f %10.2f",
				Double.parseDouble(values[0].toString()),
				Double.parseDouble(values[1].toString()),
				Double.parseDouble(values[2].toString())
			);
		}
	}

	public static class MyMapper extends Mapper<Object, Text, Text, DoubleArrayWritable> {

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			if (!value.toString().contains("["))
			{
				return; // skip rows in the input that don't have data
			}
			
			String[] pages = value.toString().split("[\\s\"\\[\\]',]+", 0);
			
			double d = .85;
			double initRank = 1.0;
			double ratio = d * initRank / (pages.length - 1);	
			
			DoubleWritable[] p0 = {
				new DoubleWritable(initRank - d),
				new DoubleWritable(pages.length - 1),
				new DoubleWritable(0)
			};
			DoubleWritable[] pn = {
				new DoubleWritable(ratio),
				new DoubleWritable(0),
				new DoubleWritable(1)
			};
			
			context.write(new Text(pages[0]), new DoubleArrayWritable(p0));

			for (int i = 1; i < pages.length; i++) {
				context.write(new Text(pages[i]), new DoubleArrayWritable(pn));
			}
		}
	}

	public static class MyReducer extends Reducer<Text, DoubleArrayWritable, Text, DoubleArrayWritable> {

		@Override
		public void reduce(Text key, Iterable<DoubleArrayWritable> values, Context context) throws IOException, InterruptedException {
			
			double[] total = {0.0, 0.0, 0.0};
			for (DoubleArrayWritable value : values) {
				DoubleWritable[] array = value.get();
				total[0] += Double.parseDouble(array[0].toString());
				total[1] += Double.parseDouble(array[1].toString());
				total[2] += Double.parseDouble(array[2].toString());
			}
			DoubleWritable[] output = {
				new DoubleWritable(total[0]),
				new DoubleWritable(total[1]),
				new DoubleWritable(total[2])
			};
			context.write(key, new DoubleArrayWritable(output));
		}
	}

	public static void main(String[] args) throws Exception {

		Job job = Job.getInstance(new Configuration());
		job.setJarByClass(PageRank.class);
		job.setJobName("Page Rank");
		
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleArrayWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleArrayWritable.class);
		
		job.setNumReduceTasks(1);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
