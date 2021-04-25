package com.demo_hadoop.service;

import com.demo_hadoop.hdfs.IHDFSDao;
import com.demo_hadoop.hdfs.impl.HDFSDaoImpl;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;

import java.util.Map;

/**
 *  支付相关服务接口类
 * @autor Sa
 * @CeeatTime 2021/3/17 15:15
 * @Version 1.0.0
 */
@Controller
public class PayService {
    //创建hdfs访问接口对象
    //IHDFSDao ihdfsDao =new HDFSDaoImpl();
    /*
    支付偏好
     */
    @RequestMapping("/paylike")
    public String paylike(Model model) throws Exception {
       try {


//           //读取分析计算数据
//           Map<String, Object> result = ihdfsDao.readFile(
//                   "/data/userimgs/output/paylike/part-r-00000");
//
  //    System.out.println(123456);
//           //封装模型数据
//           // model.addAllAttributes(result);
//           //返回展示页面路径
//           model.addAttribute("alipay",result.get("支付宝"));
//           model.addAttribute("wechat",result.get("微信"));
//           model.addAttribute("bank",result.get("银联"));
//           model.addAttribute("money",result.get("现金"));

           return "paylike";
       }
       catch (
               Exception e
       )
       {
           return  "error";
       }
    }


}
