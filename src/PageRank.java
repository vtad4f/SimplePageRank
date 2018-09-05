import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class PageRank {

	public static class MyMapper extends Mapper<Object, Text, IntWritable, Text> {

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			String[] nodes = value.toString().split(",");
			int nodeId = Integer.parseInt(nodes[0]);
			context.write(new IntWritable(nodeId), new Text(String.valueOf(0.0)));
			
			int[] outLinks = new int[nodes.length-1];			
			for(int i=1; i<nodes.length; i++) {
				outLinks[i-1] = Integer.parseInt(nodes[i]);
			}						
			double initRank = 1.0;
			double ratio = initRank / outLinks.length;			
			for (int i = 0; i < outLinks.length; i++) {
				context.write(new IntWritable(outLinks[i]), new Text(String.valueOf(ratio)));
			}
		}
	}

	public static class MyReducer extends Reducer<IntWritable, Text, IntWritable, DoubleWritable> {

		@Override
		public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			
			double pagerank = 0.0;
			for (Text value : values) {
				pagerank += Double.parseDouble(value.toString());
			}
			context.write(key, new DoubleWritable(pagerank));
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
		
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		
		job.setNumReduceTasks(1);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}