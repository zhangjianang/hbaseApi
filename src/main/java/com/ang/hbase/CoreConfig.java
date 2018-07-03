package com.ang.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.InputStream;
import java.util.Properties;

/**
 * Created by adimn on 2018/6/25.
 */
public class CoreConfig {
    private static final String PROPERTIES = "core.properties";
    public static Properties pro =  new Properties();
    public static String HBASE_QUROM = null;
    public static Connection conn;

    static {
        ClassLoader loader = Thread.currentThread().getClass().getClassLoader();
        if(loader == null){
            loader = CoreConfig.class.getClassLoader();
        }
        System.out.println("加载core 配置");
        try {
            InputStream in = loader.getResourceAsStream(PROPERTIES);
            pro.load(in);
        }catch (Exception e){
            e.printStackTrace();
        }
        HBASE_QUROM = pro.getProperty("hbase.quorum");


        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", CoreConfig.HBASE_QUROM);
        try {
            conn = ConnectionFactory.createConnection(conf);
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
