package cn.itcast.hadoop.mr.dc;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DataCount {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Configuration conf=new Configuration();
		Job job=Job.getInstance(conf);
		job.setJarByClass(DataCount.class);
		job.setMapperClass(DCMapper.class);
		//k2 v2 和 k3 v3类型对应时可以省略
		//job.setMapOutputKeyClass(Text.class);
		//job.setMapOutputValueClass(DataBean.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		
		job.setReducerClass(DCReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DataBean.class);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setPartitionerClass(ProviderPartitioner.class);
		job.setNumReduceTasks(Integer.parseInt(args[2]));  //设置reducer数量
		job.waitForCompletion(true);
	}
	
	public static class ProviderPartitioner extends Partitioner<Text, DataBean>{
		//对象初始化后，静态从上往下自动执行
		private static Map<String,Integer>providerMap=new HashMap<String, Integer>();
		
		static{
			providerMap.put("135", 1);
			providerMap.put("136", 1);
			providerMap.put("137", 1);
			providerMap.put("138", 1);
			providerMap.put("139", 1);
			providerMap.put("150", 2);
			providerMap.put("159", 2);
			providerMap.put("182", 3);
			providerMap.put("183", 3);
		}
		@Override
		public int getPartition(Text key, DataBean vale, int numPartitions) {    //返回int为分区号
			String account=key.toString();
			String sub_acc=account.substring(0,3);//从0开始取，取三位，从而得知是哪个运营商
			Integer code =providerMap.get(sub_acc);
			if(code ==null){
				code =0;
			}
			return code;
		}
		
	}
	
	public static class DCMapper extends Mapper<LongWritable, Text, Text, DataBean>{

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			//接收数据
			String line=value.toString();
			String[] fields=line.split("\t");
			String telNo=fields[1];
			long up=Long.parseLong(fields[8]);
			long down=Long.parseLong(fields[9]);
			//封装
			DataBean bean=new DataBean(telNo, up, down);
			//发送，输出
			context.write(new Text(telNo), bean);	
		}
	}
	
	public static class DCReducer extends Reducer<Text, DataBean, Text, DataBean>{

		@Override
		protected void reduce(Text key, Iterable<DataBean> v2s, Context context)
				throws IOException, InterruptedException {
			long up_sum=0;
			long down_sum=0;
			for(DataBean bean:v2s){
				up_sum+=bean.getUpPayLoad();
				down_sum+=bean.getDownPayLoad();
			}
			DataBean bean=new DataBean("", up_sum, down_sum);
			context.write(key, bean);
		}
		
	}
}
