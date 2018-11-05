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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Unionion{
	//如果两条记录line完全相同，即为相同记录，此处只需要读取为String即可比较
	//map<line,NullWritable>,reduce输出所有key即可
	
	public static class UnionionMap extends Mapper<LongWritable,Text,Text,NullWritable>{
		private Text text = new Text();
		@Override
		public void map(LongWritable offSet, Text line, Context context) throws IOException,InterruptedException{
			text.set(line);
			context.write(text, NullWritable.get());
		}
	}
	
	public static class UnionionReduce extends Reducer<Text,NullWritable,Text,NullWritable>{
		@Override
		public void reduce(Text key, Iterable<NullWritable> value, Context context) throws IOException,InterruptedException{
			context.write(key, NullWritable.get());
		}
	}
	
	public static void main(String[] args) throws Exception{
		if(args.length != 3) {
			System.err.println("Usage:Unionion <inputrelation1> <inputrelation2> <outputpath>");
			System.exit(2);
		}

		Configuration conf = new Configuration();
		
		Job Unionionjob  =Job.getInstance(conf,"Unionionjob");
		Unionionjob.setJarByClass(Unionion.class);
		Unionionjob.setMapperClass(UnionionMap.class);
		Unionionjob.setCombinerClass(UnionionReduce.class);
		Unionionjob.setReducerClass(UnionionReduce.class);
		Unionionjob.setOutputKeyClass(Text.class);
		Unionionjob.setOutputValueClass(NullWritable.class);
		FileInputFormat.setInputPaths(Unionionjob,new Path(args[0]),new Path(args[1]));
		FileOutputFormat.setOutputPath(Unionionjob,new Path(args[2]));
		
		System.exit(Unionionjob.waitForCompletion(true)?0:1);
		
	}
}