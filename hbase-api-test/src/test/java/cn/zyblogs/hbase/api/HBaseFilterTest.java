package cn.zyblogs.hbase.api;

import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

/**
 * @author Yibo Zhang
 * @date 2019/05/05
 */
public class HBaseFilterTest {

    @Test
    public void createTable() throws IOException {
        HBaseUtil.createTable("hixTable", new String[]{"brocastInfo", "saveInfo"});
        System.out.println("HBaseUtilTest.createTable ok ");
    }

    @Test
    public void addFileDetails() {

//        brocastInfo 列族
        HBaseUtil.putRow("hixTable", "rowKey3", "brocastInfo", "name", "file3.txt");
        HBaseUtil.putRow("hixTable", "rowKey3", "brocastInfo", "type", "txt");
        HBaseUtil.putRow("hixTable", "rowKey3", "brocastInfo", "size", "1024");

//        saveInfo 列族
        HBaseUtil.putRow("hixTable", "rowKey3", "saveInfo", "creator", "zhangyibo");

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
    public void rowFilterTest() {
        /**
         *  比较运算符  rowKey1 的数据
         */
        Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("rowKey1")));

//        MUST_PASS_ALL 当前的 filter 必须全部通过才算通过
        /**
         * Arrays.asList（）vs Collections.singletonList（）
         * Collections.singletonList(something)是不可变的，
         * 对Collections.singletonList(something)
         * 返回的列表所做的任何更改将导致UnsupportedOperationException 。
         * Arrays.asList(something)允许Arrays.asList(something) 更改  。
         *
         * 由Collections.singletonList(something)返回的List的容量将始终为1，
         *   而Arrays.asList(something)的容量将为已支持数组的大小。
         *
         */
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL, Collections.singletonList(filter));

        ResultScanner scanner = HBaseUtil.getScanner("hixTable", "rowKey1", "rowKey3", filterList);

        assert scanner != null;
        scanner.forEach(result -> {
//        获取主键 rowKey1
            System.out.println("rowKey = " + Bytes.toString(result.getRow()));

//            传入 列族 和 列标识 名称
            System.out.println("fileName = " + Bytes.toString(
                    result.getValue(Bytes.toBytes("brocastInfo"), Bytes.toBytes("name"))
            ));
        });
        scanner.close();
    }

    @Test
    public void prefixFilterTest() {

        Filter filter = new PrefixFilter(Bytes.toBytes("rowKey2"));
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL, Collections.singletonList(filter));

        ResultScanner scanner = HBaseUtil.getScanner("hixTable", "rowKey1", "rowKey3", filterList);
        assert scanner != null;
        scanner.forEach(result -> {
//        获取主键 rowKey1
            System.out.println("rowKey = " + Bytes.toString(result.getRow()));

//            传入 列族 和 列标识 名称
            System.out.println("fileName = " + Bytes.toString(
                    result.getValue(Bytes.toBytes("brocastInfo"), Bytes.toBytes("name"))
            ));
        });
        scanner.close();
    }

    @Test
    public void keyOnlyFilterTest() {
        //  只返回 rowkey 和 列的值 不会返回真正保存的值
        Filter filter = new KeyOnlyFilter(true);
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL, Arrays.asList(filter));
        ResultScanner scanner = HBaseUtil
                .getScanner("hixTable", "rowKey1", "rowKey3", filterList);

        assert scanner != null;
        scanner.forEach(result -> {
            System.out.println("rowKey=" + Bytes.toString(result.getRow()));
            System.out.println("fileName=" + Bytes
                    .toString(result.getValue(Bytes.toBytes("brocastInfo"), Bytes.toBytes("name"))));
        });
        scanner.close();
    }

    @Test
    public void columnPrefixFilterTest() {
//        按照列名前缀过滤  name 的属性值
        Filter filter = new ColumnPrefixFilter(Bytes.toBytes("nam"));
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL, Collections.singletonList(filter));
        ResultScanner scanner = HBaseUtil
                .getScanner("hixTable", "rowKey1", "rowKey3", filterList);

        if (scanner != null) {
            scanner.forEach(result -> {
                System.out.println("rowKey=" + Bytes.toString(result.getRow()));
                System.out.println("fileName=" + Bytes
                        .toString(result.getValue(Bytes.toBytes("brocastInfo"), Bytes.toBytes("name"))));
                System.out.println("fileType=" + Bytes
                        .toString(result.getValue(Bytes.toBytes("brocastInfo"), Bytes.toBytes("type"))));
            });
            scanner.close();
        }
    }
}
