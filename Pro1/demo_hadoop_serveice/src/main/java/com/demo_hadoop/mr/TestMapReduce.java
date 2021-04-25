package com.demo_hadoop.mr;

import com.demo_hadoop.hdfs.IHDFSDao;
import com.demo_hadoop.hdfs.impl.HDFSDaoImpl;

import java.io.IOException;

/**
 * 执行
 * @autor Sa
 * @CeeatTime 2021/3/17 14:45
 * @Version 1.0.0
 */
public class TestMapReduce {
    public static  void main(String[] args) throws Exception {

//        IHDFSDao ihdfsDao=new HDFSDaoImpl();
//        ihdfsDao.uploadFile("","hdfs://hadoop46:9000/data/userimgs/input/t2.txt");
//        (new RelationDriver()).run( "hdfs://hadoop46:9000/data/userimgs/input/t2.txt",
//                "hdfs://hadoop46:9000/data/userimgs/output/relationanly");
//        (new ClassDriver()).run( "hdfs://hadoop46:9000/data/userimgs/input/t2.txt",
//                "hdfs://hadoop46:9000/data/userimgs/output/classanly");
//        (new ParentsSatisDriver()).run( "hdfs://hadoop46:9000/data/userimgs/input/t2.txt",
//                "hdfs://hadoop46:9000/data/userimgs/output/parentsatisanly");
//        (new SexAnalyse()).run( "hdfs://hadoop46:9000/data/userimgs/input/t2.txt",
//                "hdfs://hadoop46:9000/data/userimgs/output/sexanly");
//        (new TopicDriver()).run( "hdfs://hadoop46:9000/data/userimgs/input/t2.txt",
//                "hdfs://hadoop46:9000/data/userimgs/output/topicanly");
//        (new GradeLevelDriver()).run( "hdfs://hadoop46:9000/data/userimgs/input/t2.txt",
//                "hdfs://hadoop46:9000/data/userimgs/output/Gradelevelanly");
//        (new CountryDriver()).run( "hdfs://hadoop46:9000/data/userimgs/input/t2.txt",
//                "hdfs://hadoop46:9000/data/userimgs/output/country");
        //        (new HandRaiseDriver()).run( "hdfs://hadoop46:9000/data/userimgs/input/t2.txt",
//                "hdfs://hadoop46:9000/data/userimgs/output/country");
//                (new StudentAbsence()).run( "hdfs://hadoop46:9000/data/userimgs/input/t2.txt",
//                "hdfs://hadoop46:9000/data/userimgs/output/studentabsence");

                (new Topic_ClassDriver()).run( "hdfs://hadoop46:9000/data/userimgs/input/t2.txt",
                "hdfs://hadoop46:9000/data/userimgs/output/T_C");
    }
}
