package relationpackage;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Difference{
	//relation name
	private static String r;
	private static String t;
	
	public static class DifferenceMap extends Mapper<LongWritable,Text,Text,Text>{
		private Text text = new Text();
		private Text relationname = new Text();
		protected void setup(Context context) throws IOException,InterruptedException{
			r = context.getConfiguration().get("r");
			t = context.getConfiguration().get("t");
		}
		public void map(LongWritable offSet, Text line, Context context) throws IOException,InterruptedException{
			FileSplit fileSplit = (FileSplit) context.getInputSplit();
		   String fileName = fileSplit.getPath().getName();
		   	
		   if(fileName.equals(r)) {
			   text.set(line);
			   relationname.set(r);
			   context.write(text, relationname);
		   }
		   else if(fileName.equals(t)) {
			   text.set(line);
			   relationname.set(t);
			   context.write(text, relationname);
		   }
		}
	}
	
	public static class DifferenceReduce extends Reducer<Text,Text,Text,NullWritable>{
		protected void setup(Context context) throws IOException,InterruptedException{
			r = context.getConfiguration().get("r");
		}
		public void reduce(Text key, Iterable<Text> value, Context context) throws IOException,InterruptedException{
			int sum = 0;
			boolean belongr = false;
			for(Text val:value) {
				if(val.toString().equals(r))
					belongr = true;
				sum += 1;
			}
			if(sum == 1 && belongr) {
				context.write(key,NullWritable.get());
			}
		}
	}
	
	public static void main(String[] args) throws Exception{
		if(args.length != 3) {
			System.err.println("Usage:Difference <inputrelation1> <inputrelation2> <outputpath>");
			System.exit(2);
		}
		Path Path1 = new Path(args[0]);
		Path Path2 = new Path(args[1]);
		Configuration conf = new Configuration();
		conf.set("r", Path1.getName());
		conf.set("t", Path2.getName());
		
		Job job  =Job.getInstance(conf,"Differencejob");
		job.setJarByClass(Difference.class);
		job.setMapperClass(DifferenceMap.class);
		job.setReducerClass(DifferenceReduce.class);
		job.setMapOutputKeyClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		FileInputFormat.setInputPaths(job,Path1,Path2);
		FileOutputFormat.setOutputPath(job,new Path(args[2]));
		
		System.exit(job.waitForCompletion(true)?0:1);
	}
}