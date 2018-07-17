package com.ang.hbase;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by adimn on 2018/7/11.
 */
public class ModeDelete {
    public static void main(String[] args) {
        try {
            ModeDelete md = new ModeDelete();
            String date = args[0];
            String tablename= args[1];

            int mode = 1000;
            if(args.length==3){
                mode = Integer.parseInt(args[2]);
            }
            Table indexTable = CoreConfig.conn.getTable(TableName.valueOf("gd:"+tablename+"_delete"));
            Table deleteTable = CoreConfig.conn.getTable(TableName.valueOf("gs:"+tablename));
            String path ="/data/gs/gs_split/"+date+"/"+tablename+"_delete_u.csv";
            md.deleteByIdsFile(deleteTable,indexTable,path,mode);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public boolean isDigit(String str){
        if( null == str || str.length() == 0 ){
            return false;
        }

        for(int i = str.length(); --i >= 0;){
            int c = str.charAt(i);
            if( c < 48 || c > 57 ){
                return false;
            }
        }

        return true;
    }

    public List<Row> getIdByConn(Table indexTable,String cf,String col,String id,List selfdeletes) throws IOException {
        List<Row> deletes = new ArrayList<>();
        Scan scan = new Scan();
        scan.setRowPrefixFilter(Bytes.toBytes(id+"_"));
        ResultScanner scanner = indexTable.getScanner(scan);
        Result next = scanner.next();
        while(next !=null){
            selfdeletes.add(new Delete(next.getRow()));
            byte[] value = next.getValue(Bytes.toBytes(cf), Bytes.toBytes(col));
            deletes.add(new Delete(value));
            next = scanner.next();
        }
        return deletes;
    }
    public void deleteByIdsFile(Table deltable,Table indextable,String path, int mode){
        File fr = new File(path);
        BufferedReader bf = null;
        try {
            bf = new BufferedReader(new FileReader(fr));
            String str ;
            int missNum = 0;
            int allNum = 0;
            int allDelNum = 0;
            List<Row> allDel = new ArrayList<>();
            List<Row> selfallDel = new ArrayList<>();
            while ((str = bf.readLine())!=null){
                if(isDigit(str)){
                    allNum++;
                    List<Row> idByConn = getIdByConn(indextable, "update", "id", str,selfallDel);
                    if(idByConn.size() ==0 ){
                        missNum++;
                    }else {
                        allDel.addAll(idByConn);
                    }
                    if(allNum> 0 && allNum % mode ==0){
                        Result[] results=new Result[allDel.size()];
                        deltable.batch(allDel,results);
                        allDel = new ArrayList<>();
                        allDelNum += results.length;
                        System.out.println("删除条数:"+results.length);

                        Result[] selfresults=new Result[selfallDel.size()];
                        deltable.batch(selfallDel,selfresults);
                        selfallDel= new ArrayList<>();
                        System.out.println("self删除条数:"+selfresults.length);
                    }
                }
            }
            if(allDel.size() >0){
                Result[] results=new Result[allDel.size()];
                deltable.batch(allDel,results);
                allDelNum += results.length;
                System.out.println("删除条数:"+results.length);

                Result[] selfresults=new Result[selfallDel.size()];
                deltable.batch(selfallDel,selfresults);
                System.out.println("self删除条数:"+selfresults.length);
            }
            System.out.println("-------------------------------------------");
            System.out.println(deltable.getName()+" 读取总条数:"+allNum);
            System.out.println(deltable.getName()+" 删除总条数:"+allDelNum);
            System.out.println(deltable.getName()+" key缺失总条数:"+missNum);
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
    }
}
