package com.demo_hadoop.hdfs.impl;

import com.demo_hadoop.hdfs.IHDFSDao;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

/**
 * HDFS文件数据访问类
 * @autor Sa
 * @CeeatTime 2021/3/16 14:09
 * @Version 1.0.0
 */
public class HDFSDaoImpl implements IHDFSDao {



    @Override
    public void uploadFile(String souceUri, String targetUri) throws Exception {
        //获得文件系统对象
        FileSystem fs = FileSystem.get(new URI(uri),configuration,user);
        //路径对象
        Path path=new Path(targetUri);
        Path task=new Path(souceUri);

        //上传文件
        fs.copyFromLocalFile(task,path);


        fs.close();
        System.out.println("Success ..upLoadfile");
    }

    @Override
    public Map<String, Object> readFile(String sourceUri) throws Exception {
        //获得文件系统对象
        FileSystem fs = FileSystem.get(new URI(uri),configuration,user);
        //路径对象
        Path path=new Path(sourceUri);
        FSDataInputStream in=fs.open(path);
        BufferedReader br=new BufferedReader(new InputStreamReader(in));

        //创建封装数据的图集合

        Map<String,Object> datas=new HashMap<>();
        //提取数据
        // 输入流
        // 创建缓冲字符输入流
        String line;
        while ((line=br.readLine())!=null)
        {
            System.out.println(line);

            String[] kv=line.split("\t");



            datas.put(kv[0],kv[1]);
        }


        br.close();
        in.close();
        fs.close();
        System.out.println("Success ..readfile");
        return datas;
    }

    @Override
    public void mkDir(String Uri) throws Exception {
        FileSystem fs = FileSystem.get(new URI(uri),configuration,user);
        //路径对象
        Path path=new Path(Uri);
        //创建目录
        fs.mkdirs(path);
        fs.close();
        System.out.println("Success..mkDir");

    }


}
