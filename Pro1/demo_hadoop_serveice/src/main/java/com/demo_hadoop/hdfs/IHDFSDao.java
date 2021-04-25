package com.demo_hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;

import java.util.Map;

/**
 * *HDFS数据访问接口
 * @autor Sa
 * @CeeatTime 2021/3/16 14:00
 * @Version 1.0.0
 */
public interface IHDFSDao {

    public static  final String uri="hdfs://hadoop46:9000";
    public static final String user="Sa";
    public static final Configuration configuration=new Configuration();



    //上传文件
    public void uploadFile(String souceUri, String targetUri)throws Exception;
    //读取文件 MapReduce计算分析结果
    public Map<String,Object> readFile(String sourceUri)throws Exception;
    //创建目录
    public void mkDir(String Uri)throws Exception;
    //数据分析



}
