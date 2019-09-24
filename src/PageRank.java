import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class PageRank {

	public static class MyMapper extends Mapper<Object, Text, Text, DoubleWritable[]> {

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			String[] pages = value.toString().split("[\\s\"\\[\\]',]");
			
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
			
			context.write(new Text(pages[0]), p0);

			for (int i = 1; i < pages.length; i++) {
				context.write(new Text(pages[i]), pn);
			}
		}
	}

	public static class MyReducer extends Reducer<Text, DoubleWritable[], Text, DoubleWritable[]> {

		@Override
		public void reduce(Text key, Iterable<DoubleWritable[]> values, Context context) throws IOException, InterruptedException {
			
			double[] total = {0.0, 0.0, 0.0};
			for (DoubleWritable[] value : values) {
				total[0] += Double.parseDouble(value[0].toString());
				total[1] += Double.parseDouble(value[1].toString());
				total[2] += Double.parseDouble(value[2].toString());
			}
			DoubleWritable[] output = {
				new DoubleWritable(total[0]),
				new DoubleWritable(total[1]),
				new DoubleWritable(total[2])
			};
			context.write(key, output);
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
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable[].class);
		
		job.setNumReduceTasks(1);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
