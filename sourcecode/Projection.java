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

public class Projection{
	/*
	 * 通过map来获取每行数据属性col的值，输出为<col值,NullWritable>
	 * 通过reduce来合并所有键值对，这里类似于WordCount的reducer，输出<col值,NullWritable>
	 * 结果即为属性col的投影
	 */
	private static String col;
	
	public static class ProjectionMap extends Mapper<LongWritable,Text,Text,NullWritable>{
		private Text val = new Text();
		
		@Override
		protected void setup(Context context) throws IOException{
			col = context.getConfiguration().get("col");
		}
		
		@Override
		public void map(LongWritable offSet, Text line, Context context) throws IOException,InterruptedException{
			RelationA record = new RelationA(line.toString());
			val.set(record.getCol(col));
			context.write(val, NullWritable.get());
		}
	}
	
	private static class ProjectionReduce extends Reducer<Text,NullWritable,Text,NullWritable>{
		
		@Override
		public void reduce(Text key, Iterable<NullWritable> value, Context context) throws IOException,InterruptedException{
			context.write(key, NullWritable.get());
		}
	}
	
	public static void main(String[] args) throws Exception{
		if(args.length != 3) {
			System.err.println("Usage:projection <columnname> <inputpath> <outputpath>");
			System.exit(2);
		}else {
			col = args[0];
		}
		Configuration conf = new Configuration();
		conf.set("col", col);
		
		Job Projectionjob  =Job.getInstance(conf,"Projectionjob");
		Projectionjob.setJarByClass(Projection.class);
		Projectionjob.setMapperClass(ProjectionMap.class);
		Projectionjob.setCombinerClass(ProjectionReduce.class);
		Projectionjob.setReducerClass(ProjectionReduce.class);
		Projectionjob.setOutputKeyClass(Text.class);
		Projectionjob.setOutputValueClass(NullWritable.class);
		FileInputFormat.addInputPath(Projectionjob,new Path(args[1]));
		FileOutputFormat.setOutputPath(Projectionjob,new Path(args[2]));
		
		System.exit(Projectionjob.waitForCompletion(true)?0:1);
		
	}
}