package relationpackage;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Intersection{
	//如果两条记录line完全相同，即为相同记录，此处只需要读取为String即可比较
	//map<line,1>,reduce合并键值对，输出所有<key,2>的key值即可
	
	public static class IntersectionMap extends Mapper<LongWritable,Text,Text,IntWritable>{
		private Text text = new Text();
		private final static IntWritable one = new IntWritable(1);
		
		@Override
		public void map(LongWritable offSet, Text line, Context context) throws IOException,InterruptedException{
			text.set(line);
			context.write(text, one);
		}
	}
	
	public static class IntersectionReduce extends Reducer<Text,IntWritable,Text,NullWritable>{
		@Override
		public void reduce(Text key, Iterable<IntWritable> value, Context context) throws IOException,InterruptedException{
			int sum = 0;
			for(IntWritable val:value) {
				sum += val.get();
			}
			if(sum == 2)
				context.write(key, NullWritable.get());
		}
	}
	
	public static void main(String[] args) throws Exception{
		if(args.length != 3) {
			System.err.println("Usage:intersection <inputrelation1> <inputrelation2> <outputpath>");
			System.exit(2);
		}

		Configuration conf = new Configuration();
		
		Job Intersectionjob  =Job.getInstance(conf,"Intersectionjob");
		Intersectionjob.setJarByClass(Intersection.class);
		Intersectionjob.setMapperClass(IntersectionMap.class);
		//Intersectionjob.setCombinerClass(IntersectionReduce.class);
		Intersectionjob.setReducerClass(IntersectionReduce.class);
		Intersectionjob.setMapOutputKeyClass(Text.class);
		Intersectionjob.setOutputKeyClass(Text.class);
		Intersectionjob.setMapOutputValueClass(IntWritable.class);
		Intersectionjob.setOutputValueClass(NullWritable.class);
		FileInputFormat.setInputPaths(Intersectionjob,new Path(args[0]),new Path(args[1]));
		FileOutputFormat.setOutputPath(Intersectionjob,new Path(args[2]));
		
		System.exit(Intersectionjob.waitForCompletion(true)?0:1);
		
	}
}