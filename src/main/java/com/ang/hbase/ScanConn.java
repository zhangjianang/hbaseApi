package com.ang.hbase;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * Created by adimn on 2018/6/27.
 */
public class ScanConn {
    public static void main(String[] args) throws IOException {
        Table table = CoreConfig.conn.getTable(TableName.valueOf(""));
        Scan scan = new Scan();
        //根据rowkey进行过滤
        Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL,new RegexStringComparator(".*京Q00"));
        scan.setFilter(filter);
        ResultScanner res =table.getScanner(scan);
        for(Result perRes:res){
            System.out.println(perRes);
        }
    }
}
