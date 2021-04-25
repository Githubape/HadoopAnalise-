package com.demo_hadoop.service;

import com.demo_hadoop.hdfs.IHDFSDao;
import com.demo_hadoop.hdfs.impl.HDFSDaoImpl;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.net.URI;
/**
 *  支付相关服务接口类
 * @autor Sa
 * @CeeatTime 2021/3/17 15:15
 * @Version 1.0.0
 */
@Controller
public class TestService {
    //创建hdfs访问接口对象
    IHDFSDao ihdfsDao =new HDFSDaoImpl();
    /*
    支付偏好
     */
    public static  final String uri="hdfs://hadoop46:9000";
    public static final String user="Sa";
    public static final Configuration configuration=new Configuration();

    @RequestMapping("/Test")
    public String queryStudentInfo(Model model) throws URISyntaxException, IOException, InterruptedException {
        List<Map<String, Object>> resultList = new ArrayList<Map<String, Object>>();
        Map<String, Object> student = new HashMap<String, Object>(){{
            put("gender", "M");
            put("GradeID", "A");
            put("Topic", "IT");
            put("Relation", "Father");
            put("Class", "M");

        }};
        resultList.add(student);



        //获得文件系统对象
        org.apache.hadoop.fs.FileSystem fs = FileSystem.get(new URI(uri),configuration,user);
        //路径对象
        Path path=new Path("/data/userimgs/input/t2.txt");
        //输入流
        FSDataInputStream in=fs.open(path);
        // 创建缓冲字符输入流
        BufferedReader br=new BufferedReader(new InputStreamReader(in,"GBK"));
        //循环读取数据
        String line;
        line=br.readLine();
        while((line=br.readLine())!=null)
        {
           // System.out.println(line);
            final String[] kv=line.split(",");

            student = new HashMap<String, Object>(){{
                put("gender", kv[0]);
                put("GradeID", kv[4]);
                put("Topic", kv[6]);
                put("Relation", kv[8]);
                put("Class", kv[16]);

            }};
            resultList.add(student);

        }



        model.addAttribute("resultList", resultList);







        br.close();
        in.close();
        fs.close();
        System.out.println("Success ..readfile");


        return "Test";
    }




}
