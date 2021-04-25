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
 * @autor Sa
 * @CeeatTime 2021/3/17 8:30
 * @Version 1.0.0
 */
public class WordcountDriver {
    /*
    Map处理
    分割及映射数据
     */

    public static class WordCountMapper extends Mapper<LongWritable, Text,Text, IntWritable>
    {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //分割单行数据
            String[] words=value.toString().split(" +");
            //循环映射数据
            for(String word:words )
            {
                System.out.println(word);
                context.write(new Text(word),new IntWritable(1));
            }
        }
    }
    /*
    Reduce 处理
    汇总数据
     */
    public static class WordCountReduce extends Reducer<Text,IntWritable,Text,IntWritable>
    {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        //计数
        int count=0;
        // 迭代提取数据
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
    public void run() throws IOException, ClassNotFoundException, InterruptedException {
        //创建配置信息对象
        Configuration configuration=new Configuration();
        //作业对象
        Job job= Job.getInstance(configuration);//Job.getInstance(configuration);
        //指定jar包
        job.setJarByClass(WordcountDriver.class);
        //指定Mapper Reducer业务处理类
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReduce.class);
        //指定map输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        //指定最终输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        //指定输出目录


        FileInputFormat.setInputPaths(job,new Path("hdfs://hadoop46:9000/data/input2/t1.txt"));//setInputPaths(job,new Path("hdfs://hadoop46:9000/data/input2/result.txt"));
        FileOutputFormat.setOutputPath(job,new Path("hdfs://hadoop46:9000/data/input2/result.txt"));
        //等待作业执行完成
        boolean result=job.waitForCompletion(true);
    }



}
