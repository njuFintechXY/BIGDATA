package relationpackage;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Selection {
	private static String col;
	private static String value;
	private static String symbol;
	/*
	//定义关系A的结构
	public static class RelationA implements WritableComparable<RelationA>{
		private int age;
		private String name;
		private int id;
		private int weight;
		
		public RelationA(){}
		public RelationA(int nid,String nname, int nage ,int nweight) {
			age = nage;
			id = nid;
			name = nname;
			weight = nweight;
		}
		public RelationA(String line) {
			String[] value = line.split(",");
			id = Integer.parseInt(value[0]);
			name = value[1];
			age = Integer.parseInt(value[2]);
			weight = Integer.parseInt(value[3]);
		}
	}*/
	
	public static class SelectionMap extends Mapper<LongWritable, Text, RelationA, NullWritable>{
	
		@Override
		protected void setup(Context context) throws IOException,InterruptedException{
			col = context.getConfiguration().get("col");
			value = context.getConfiguration().get("value");
			symbol = context.getConfiguration().get("symbol");
		}
		
		@Override
		public void map(LongWritable offSet, Text line, Context context)throws 
		IOException, InterruptedException{
			RelationA record = new RelationA(line.toString());
			if(record.isCondition(col,symbol,value))
				context.write(record, NullWritable.get());
		}
	}
	public static void main(String[] args) throws Exception{
		if(args.length != 5) {
			System.err.println("Usage:Selection <columnname> <symbol> <value> <inputpath> <outputpath>");
			System.exit(2);
		}else {
			col = args[0];
			symbol = args[1];
			value = args[2];
		}
		Configuration conf = new Configuration();
		conf.set("col", col);
		conf.set("symbol", symbol);
		conf.set("value", value);
		
		Job selectionJob = Job.getInstance(conf,"selectionJob");
		selectionJob.setJarByClass(Selection.class);
		selectionJob.setMapperClass(SelectionMap.class);
		selectionJob.setMapOutputKeyClass(RelationA.class);
		selectionJob.setMapOutputValueClass(NullWritable.class);

		selectionJob.setNumReduceTasks(0);
		FileInputFormat.addInputPath(selectionJob, new Path(args[3]));
		FileOutputFormat.setOutputPath(selectionJob, new Path(args[4]));
		
		System.exit(selectionJob.waitForCompletion(true)?0:1);
	}
}