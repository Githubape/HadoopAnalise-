package com.demo_hadoop.service;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;

/**
 * 处理路径转发服务类
 * @autor Sa
 * @CeeatTime 2021/3/17 14:00
 * @Version 1.0.0
 */
@Controller
public class ParhService {
    @RequestMapping("/")
    public String toIndex()
    {
        return  "index";
    }

}
