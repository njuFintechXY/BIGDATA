package relationpackage;

import java.io.IOException;
import java.util.ArrayList;

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

public class NaturalJoin{
	//relation name
	private static String r;
	private static String t;
		
	public static class NaturalJoinMap extends Mapper<LongWritable, Text, Text, Text>{
		private Text col = new Text();//键
		private Text last = new Text();//由relationname和剩余属性组成的value
		protected void setup(Context context) throws IOException,InterruptedException{
			r = context.getConfiguration().get("r");
			t = context.getConfiguration().get("t");
		}
		public void map(LongWritable offSet, Text line, Context context) throws IOException,InterruptedException{
			FileSplit fileSplit = (FileSplit) context.getInputSplit();
			String fileName = fileSplit.getPath().getName();
			
			String[] linesplit = line.toString().split(",",2);
			col.set(linesplit[0]);
			if(fileName.equals(r)) {
				last.set(r+","+linesplit[1]);
			}
			else if(fileName.equals(t)) {
				last.set(t+","+linesplit[1]);
			}
			context.write(col, last);
		}
	}
	
	public static class NaturalJoinReduce extends Reducer<Text,Text,Text,NullWritable>{
		protected void setup(Context context) throws IOException,InterruptedException{
			r = context.getConfiguration().get("r");
			t = context.getConfiguration().get("t");
		}
		public void reduce(Text key, Iterable<Text> value,Context context) throws IOException,InterruptedException{
			ArrayList<Text> setR = new ArrayList<Text>();
			ArrayList<Text> setT = new ArrayList<Text>();
			//按照来源分为两组然后做笛卡尔乘积
			for(Text val : value){
				String[] recordInfo = val.toString().split(",",2);
				if(recordInfo[0].equals(r))
					setR.add(new Text(recordInfo[1]));
				else
					setT.add(new Text(recordInfo[1]));
			}
			for(int i = 0;i<setR.size();i++)
				for(int j = 0;j<setT.size();j++) {
					Text t = new Text(key.toString()+","+setR.get(i).toString()+","+setT.get(j).toString());
					context.write(t, NullWritable.get());
				}
		}
	}
	
	public static void main(String[] args) throws Exception{
		if(args.length != 3) {
			System.err.println("Usage:naturaljoin <inputrelation1> <inputrelation2> <outputpath>");
			System.exit(2);
		}
		Path Path1 = new Path(args[0]);
		Path Path2 = new Path(args[1]);
		Configuration conf = new Configuration();
		conf.set("r", Path1.getName());
		conf.set("t", Path2.getName());
		
		Job job  =Job.getInstance(conf,"NaturalJoinjob");
		job.setJarByClass(NaturalJoin.class);
		job.setMapperClass(NaturalJoinMap.class);
		job.setReducerClass(NaturalJoinReduce.class);
		job.setMapOutputKeyClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		FileInputFormat.setInputPaths(job,Path1,Path2);
		FileOutputFormat.setOutputPath(job,new Path(args[2]));
		
		System.exit(job.waitForCompletion(true)?0:1);
	}
}