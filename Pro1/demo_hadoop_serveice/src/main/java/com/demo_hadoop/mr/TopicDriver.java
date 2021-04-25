package com.demo_hadoop.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 专业分析与统计
 * @autor Sa
 * @CeeatTime 2021/3/17 14:23
 * @Version 1.0.0
 */
public class TopicDriver {
     /*
    Map处理
    分割及映射数据
     */

    public static class TopicMapper extends Mapper<LongWritable, Text,Text, IntWritable>
    {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //获取单行数据
            String line=value.toString();
            //分割字符串
            String[] fields=line.split(",");
            //判断是否存在字段
            if(fields!=null&&fields.length>0);

            //获得性别
            String payType= fields[fields.length-11];
            //输出数据
            context.write(new Text(payType),new IntWritable(1));
        }
    }
    /*
    Reduce 处理
    汇总数据
     */
    public static class TopicReduce extends Reducer<Text,IntWritable,Text,IntWritable>
    {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            //声明计数变量
            int count=0;
            //遍历提取数据
            for(IntWritable i:values)
            {
                count++;
            }
            //输出数据
            context.write(key,new IntWritable(count));


        }

    }
    /*
    执行接口
     */
    public void run(String in,String out) throws IOException, ClassNotFoundException, InterruptedException {
        //创建配置信息对象
        Configuration configuration=new Configuration();
        //作业对象
        Job job= Job.getInstance(configuration);//Job.getInstance(configuration);
        //指定jar包
        job.setJarByClass(TopicDriver.class);
        //指定Mapper Reducer业务处理类
        job.setMapperClass(TopicMapper.class);
        job.setReducerClass(TopicReduce.class);
        //指定map输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        //指定最终输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        //指定输出目录


//        FileInputFormat.setInputPaths(job,new Path(
//                "hdfs://hadoop46:9000/data/userimgs/input/t2.txt"));//setInputPaths(job,new Path("hdfs://hadoop46:9000/data/input2/result.txt"));
//        FileOutputFormat.setOutputPath(job,new Path(
//                "hdfs://hadoop46:9000/data/userimgs/output/Topicanly"));

        FileInputFormat.setInputPaths(job,new Path(in));//setInputPaths(job,new Path("hdfs://hadoop46:9000/data/input2/result.txt"));
        FileOutputFormat.setOutputPath(job,new Path(out));//"hdfs://hadoop46:9000/data/userimgs/output/classanly"));

        //等待作业执行完成
        boolean result=job.waitForCompletion(true);
        System.out.println("finish____analise");
    }



}
