package com.ang.hbase;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by adimn on 2018/6/28.
 */
public class TestScan {
    public static void oldInvoke(String[] args) {
        if(args.length<4){
            System.out.println("输入四个参数：表名，列簇名，列名，删除Id路径");
            System.exit(1);
        }
//        List<String> ids = BatchDelete.getIds(args[3]);
        List<String> ids = new ArrayList<>();
        List<Row> alldel = new ArrayList<>();

        try {
            Table table = CoreConfig.conn.getTable(TableName.valueOf(args[0]));
            for(String id:ids) {
                alldel.addAll(selectByFilter(table, args[1], args[2],id));
            }
            Result[] results=new Result[alldel.size()];
            table.batch(alldel, results);
            System.out.println("删除数目："+alldel.size());
            table.close();
            CoreConfig.conn.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static  List<Row> selectByFilter(Table table,String cf,String cname,String id) throws Exception {
        FilterList filterList = new FilterList();
        Scan s1 = new Scan();


        filterList.addFilter(new SingleColumnValueFilter(Bytes.toBytes(cf),
                        Bytes.toBytes(cname),
                        CompareFilter.CompareOp.EQUAL,Bytes.toBytes(id)
                )
        );
        // 添加下面这一行后，则只返回指定的cell，同一行中的其他cell不返回
//
        s1.setFilter(filterList);

        ResultScanner ResultScannerFilterList = table.getScanner(s1);

        List<Row> batchs=new ArrayList<>();
        int num = 0;
        for(Result rr=ResultScannerFilterList.next();rr!=null;rr=ResultScannerFilterList.next()){
            byte[] key = rr.getRow();
            System.out.println("key : "+ new String(key));
            if(key == null || key.length==0){
                continue;
            }
            Delete delete=new Delete(key);
            batchs.add(delete);
            num++;
        }
        return batchs;
    }


    public static void deleteBatch(Table table,Result[] results,String cf,String col) throws IOException, InterruptedException {
        List<Row> deletes = new ArrayList<>();
        for(int i = 0; i <results.length; i++){
            String key = new String(results[i].getRow());
            byte[] value = results[i].getValue(Bytes.toBytes(cf), Bytes.toBytes(col));
            Delete delete = new Delete(value);
            deletes.add(delete);
        }
        Result[] res = new Result[deletes.size()];
        table.batch(deletes,res);
        System.out.println("删除条数："+deletes.size());
    }
}
