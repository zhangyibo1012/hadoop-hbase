package cn.zyblogs.hbase.api;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.io.IOException;

/**
 * @author Yibo Zhang
 * @date 2019/04/30
 */
public class HBaseUtilTest {

    @Test
    public void createTable() throws IOException {
        HBaseUtil.createTable("system3RecommendTable", new String[]{"broadcastInfo"});
        System.out.println("HBaseUtilTest.createTable ok ");
    }

    @Test
    public void addFileDetails() {

//        broadcastInfo 列族
        HBaseUtil.putRow("systemRecommendTable", "rowKey1", "broadcastInfo", "broadcastId", "1");
        HBaseUtil.putRow("systemRecommendTable", "rowKey1", "broadcastInfo", "broadcastWeight", "70");
        HBaseUtil.putRow("systemRecommendTable", "rowKey1", "broadcastInfo", "handlerUserId", "60001");
        HBaseUtil.putRow("systemRecommendTable", "rowKey1", "broadcastInfo", "handlerUserId", "60001");
        HBaseUtil.putRow("systemRecommendTable", "rowKey1", "broadcastInfo", "intimacy", "");
        HBaseUtil.putRow("systemRecommendTable", "rowKey1", "broadcastInfo", "intimacy", "");
        HBaseUtil.putRow("systemRecommendTable", "rowKey1", "broadcastInfo", "intimacy", "");
        HBaseUtil.putRow("systemRecommendTable", "rowKey1", "broadcastInfo", "intimacy", "");
        HBaseUtil.putRow("systemRecommendTable", "rowKey1", "broadcastInfo", "intimacy", "");

////        saveInfo 列族
//        HBaseUtil.putRow("hixTable", "rowKey1", "saveInfo", "creator", "zhangyibo");
//
////        brocastInfo 列族
//        HBaseUtil.putRow("hixTable", "rowKey2", "brocastInfo", "name", "file2.jpg");
//        HBaseUtil.putRow("hixTable", "rowKey2", "brocastInfo", "type", "jpg");
//        HBaseUtil.putRow("hixTable", "rowKey2", "brocastInfo", "size", "1024");
//
////        saveInfo 列族
//        HBaseUtil.putRow("hixTable", "rowKey2", "saveInfo", "creator", "zhangyibo");
        System.out.println("true = " + true);
    }

    @Test
    public void getFileDetails() {
//        查询 rowKey1 的值
        Result result = HBaseUtil.getRow("hixTable", "rowKey1");

//            获取主键 rowKey1
        System.out.println("rowKey = " + Bytes.toString(result.getRow()));

//            传入 列族 和 列标识 名称
        System.out.println("fileName = " + Bytes.toString(
                result.getValue(Bytes.toBytes("brocastInfo"), Bytes.toBytes("name"))
        ));
    }

    @Test
    public void scanFileDetails() {
        ResultScanner scanner = HBaseUtil.getScanner("hixTable", "rowKey1", "rowKey2");
        if (scanner != null) {
            scanner.forEach(result -> {
//                获取主键 rowKey1
                System.out.println("rowKey = " + Bytes.toString(result.getRow()));

//            传入 列族 和 列标识 名称
                System.out.println("fileName = " + Bytes.toString(
                        result.getValue(Bytes.toBytes("brocastInfo"), Bytes.toBytes("name"))
                ));
            });
            scanner.close();
        }
    }

    @Test
    public void deleteRow() {
//        删除 rowKey1 一行 数据
        HBaseUtil.deleteRow("hixTable", "rowKey1");
        System.out.println("true = " + true);
    }

    @Test
    public void deleteTable() {
        boolean hixTable = HBaseUtil.deleteTable("hixTable");
        System.out.println("hixTable = " + hixTable);
    }
}
