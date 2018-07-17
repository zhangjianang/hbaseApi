package com.ang.hbase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by adimn on 2018/6/25.
 */
public class table_scan {
    public static void main(String[] args) throws Exception {

        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", CoreConfig.HBASE_QUROM);
        Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf("gs:company_change_info"));

        List<String> arr=new ArrayList<String>();
        arr.add("update,name,20");
//        arr.add("course,math,100");
        selectByFilter(table,arr);
        connection.close();
    }

    public static void simple_scan() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", CoreConfig.HBASE_QUROM);
        Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf(Constants.TABLE_NAME));

        Scan scan = new Scan();
        scan.addColumn(Constants.COLUMN_FAMILY_DF.getBytes(), "name".getBytes());
//        scan.addFamily(Constants.COLUMN_FAMILY_EX.getBytes());

        ResultScanner rs = table.getScanner(scan);
        for (Result r = rs.next(); r != null; r = rs.next()) {
            byte[] row_key = r.getRow();
            System.out.print("[------]row_key=" + new String(row_key) + "\n");
            byte[] name = r.getValue(Constants.COLUMN_FAMILY_DF.getBytes(), "name".getBytes());
            System.out.print("[------]name=" + new String(name) + "\n");
//            byte[] weight = r.getValue(Constants.COLUMN_FAMILY_EX.getBytes(), "weight".getBytes());
//            System.out.print("[------]weight=" + new String(weight) + "\n");
        }
        table.close();
        connection.close();
    }

    public static void batch_try(Table table) throws Exception {

//        FilterList filterList = new FilterList();
        Scan scan = new Scan();
        SingleColumnValueFilter filter = new SingleColumnValueFilter(Bytes.toBytes("name"), Bytes.toBytes("name"), CompareFilter.CompareOp.EQUAL, new SubstringComparator("ang"));
        scan.setFilter(filter);

        ResultScanner results = table.getScanner(scan);
        Result next = results.next();
        table.close();
    }

    public static void selectByFilter(Table table,List<String> arr) throws Exception {
        FilterList filterList = new FilterList();
        Scan s1 = new Scan();
        for(String v:arr){ // 各个条件之间是“与”的关系
            String [] s=v.split(",");
            filterList.addFilter(new SingleColumnValueFilter(Bytes.toBytes(s[0]),
                            Bytes.toBytes(s[1]),
                            CompareFilter.CompareOp.EQUAL,Bytes.toBytes(s[2])
                    )
            );
            // 添加下面这一行后，则只返回指定的cell，同一行中的其他cell不返回
//          s1.addColumn(Bytes.toBytes(s[0]), Bytes.toBytes(s[1]));
        }
        s1.setFilter(filterList);
        ResultScanner ResultScannerFilterList = table.getScanner(s1);

        List<Row> batchs=new ArrayList<>();
        int num = 0;
        for(Result rr=ResultScannerFilterList.next();rr!=null;rr=ResultScannerFilterList.next()){
            System.out.println("key : "+new String(rr.getRow()));
            Delete delete=new Delete(rr.getRow());
            batchs.add(delete);
            num++;
        }
        //创建结果集
        Result[] results=new Result[num];
        table.batch(batchs, results);
        System.out.println("delete done!");
    }

}
