package com.ang.hbase;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by adimn on 2018/6/25.
 */
public class BatchDelete {

    public static void main(String[] args) {
        BatchDelete bd = new BatchDelete();
        try {
            List<String> ids = bd.getIds(args[0]);
            Table indexTable = CoreConfig.conn.getTable(TableName.valueOf("gd:company_change_info_delete"));
            Table deleteTable = CoreConfig.conn.getTable(TableName.valueOf("gs:company_change_info"));
            bd.deleteById(indexTable,deleteTable,ids);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }



    public  List<String> getIds(String path){
        List<String> ids = new ArrayList<>();
        File fr = new File(path);
        BufferedReader bf = null;
        try {
            bf = new BufferedReader(new FileReader(fr));
            String str = null;
            while ((str = bf.readLine())!=null){
                if(str != null && str !=""){
                    ids.add(str);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            if(bf !=null){
                try {
                    bf.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        System.out.println("获取id条数："+ids.size());
        return ids;
    }


    public  void deleteById(Table indexTable,Table deleteTable,List ids) throws IOException, InterruptedException {
        List<Row> deletes = new ArrayList<>();
        int mode = 500;
        for(int i=0;i<ids.size();i++) {
            Object id = ids.get(i);
            Scan scan = new Scan();
            scan.setRowPrefixFilter(Bytes.toBytes(id.toString()+"_"));
            ResultScanner scanner = indexTable.getScanner(scan);
            Result next = scanner.next();
            while(next !=null){
                byte[] value = next.getValue(Bytes.toBytes("update"), Bytes.toBytes("id"));
                deletes.add(new Delete(value));
                next = scanner.next();
            }
            if(i >0 && i % mode ==0){
                Result[] results = new Result[deletes.size()];
                deleteTable.batch(deletes,results);
                System.out.println("删除条数："+deletes.size());
                deletes = new ArrayList();
            }
        }
        if(deletes.size() > 0){
            Result[] results=new Result[deletes.size()];
            deleteTable.batch(deletes,results);
            System.out.println("删除条数："+deletes.size());
        }
    }






}
