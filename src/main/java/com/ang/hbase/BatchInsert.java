package com.ang.hbase;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.ang.hbase.Constants.*;

/**
 * Created by adimn on 2018/6/26.
 */
public class BatchInsert {
    public static void main(String[] args) {
        try {
            List<String> tables = new ArrayList<String>();
            tables.addAll(COMPANY_CONN);
            tables.addAll(REPORT_CONN);
            tables.addAll(REPORT_S_CONN);
            for(String tablename:tables){
//                System.out.println(tablename+"_delete 增加数据");

//                Table table = CoreConfig.conn.getTable(TableName.valueOf("ns1:"+tablename));
//                Table insertTable = CoreConfig.conn.getTable(TableName.valueOf(tablename+"_delete"));
//                batchScan(table,insertTable,"update",getColByTableName(tablename));
//                table.close();
//                insertTable.close();
            }
            Table table = CoreConfig.conn.getTable(TableName.valueOf("ns1:company_change_info"));
            Table insertTable = CoreConfig.conn.getTable(TableName.valueOf("company_change_info_delete"));
            pageScan(table,insertTable,"update",getColByTableName("company_change_info"),Integer.parseInt(args[0]));
            table.close();
            insertTable.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    private static String getColByTableName(String table){
        if(COMPANY_CONN.contains(table)){
            return "company_id";
        }else if(REPORT_CONN.contains(table)){
            return "annualreport_id";
        }else if(REPORT_S_CONN.contains(table)){
            return "annual_report_id";
        }
        return "id";
    }

    public static List batchScan(Table table,Table insertTable,String cf,String col) throws IOException {

        Scan scan = new Scan();
//        String cf = "update";
//        String col ="annualreport_id";
        List allIds = new ArrayList<>();
//        scan.addColumn(Bytes.toBytes(cf), Bytes.toBytes(col));

        scan.addColumn(Bytes.toBytes(cf), Bytes.toBytes(col))
                .setStartRow(Bytes.toBytes("000000000")).setStopRow(Bytes.toBytes("000010000"));
        ResultScanner scanner = table.getScanner(scan);
        int num =0;
        for (Result res : scanner){
            num++;
            List per = new ArrayList<>();
            byte[] key = res.getRow();
            byte[] annid = res.getValue(cf.getBytes(), col.getBytes());
            StringBuilder sb = new StringBuilder();
            sb.append(new String(annid)).append("_").append(new String(key));
            per.add(sb.toString());
            per.add(new String(key));
            allIds.add(per);
            if(num%2000 ==0){
                batchSave(insertTable, "info", "id", allIds);
                allIds = new ArrayList();
            }
        }
        if(allIds.size()>0){
            batchSave(insertTable, "info", "id", allIds);
        }
        System.out.println("数目："+allIds.size());
        scanner.close();
        return allIds;
    }

    public static void pageScan(Table table,Table insertTable,String cf,String col,int pagenum) throws IOException {
        List allIds = new ArrayList<>();
        byte[] POSTFIX = new byte[] { 0x00 };//长度为零的字节数组
        Filter filter = new PageFilter(pagenum);//设置一页所含的行数
        int totalRows = 0;//总行数
        byte[] lastRow = null;//该页的最后一行
        while (true) {
            Scan scan = new Scan();
            scan.setCaching(pagenum);//取1000条记录
            scan.setFilter(filter);
            //如果不是第一页
            if (lastRow != null) {
                // 因为不是第一页，所以我们需要设置其实位置，我们在上一页最后一个行键后面加了一个零，来充当上一页最后一个行键的下一个
//                byte[] startRow = Bytes.add(lastRow, POSTFIX);
                byte[] startRow = lastRow;
                System.out.println("start row: "
                        + Bytes.toStringBinary(startRow));
                scan.setStartRow(startRow);
            }
            ResultScanner scanner = table.getScanner(scan);
            int localRows = 0;
            Result result;
            while ((result = scanner.next()) != null) {
//                System.out.println(localRows++ + ": " + result);
                localRows++;
                List per = new ArrayList<>();
                byte[] key = result.getRow();
                byte[] annid = result.getValue(cf.getBytes(), col.getBytes());
                StringBuilder sb = new StringBuilder();
                sb.append(new String(annid)).append("_").append(new String(key));
                per.add(sb.toString());
                per.add(new String(key));
                allIds.add(per);

                totalRows++;
                lastRow = result.getRow();//获取最后一行的行键
            }
            batchSave(insertTable, "info", "id", allIds);
            scanner.close();
            //最后一页，查询结束
            if (localRows == 0)
                break;
        }
        System.out.println("total rows: " + totalRows);
    }

    public static void batchSave(Table table, String cf, String col, List<List> data) throws IOException {
        List<Put> puts = new ArrayList<>();
        for(List row : data) {
            if(row.size()!=2){
                System.out.println("缺失信息");
                continue;
            }
            Put put = new Put(Bytes.toBytes(row.get(0).toString()));
            put.addColumn(cf.getBytes(), col.getBytes(), Bytes.toBytes(row.get(1).toString()));
            puts.add(put);
        }
        table.put(puts);
        System.out.println("put数目："+puts.size());
    }
}
