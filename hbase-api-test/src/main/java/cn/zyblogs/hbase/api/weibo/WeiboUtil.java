package cn.zyblogs.hbase.api.weibo;

import cn.zyblogs.hbase.api.HBaseConnection;
import cn.zyblogs.hbase.api.HBaseUtil;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author Yibo Zhang
 * @date 2019/05/14
 */
public class WeiboUtil {

    /**
     *  创建命名空间
     */
    public static void createNamespace(String namespace) throws IOException {

        Admin admin =HBaseConnection.getHBaseConnection().getAdmin();

//        创建 ns 描述器
        NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(namespace).build();

//        创建操作
        admin.createNamespace(namespaceDescriptor);

//        关闭资源
        admin.close();
        HBaseConnection.closeConnection();
    }

    /**
     *  创建表
     */
    public static void createTable(String tableName,int versions , String... cfs) throws IOException {

        Admin admin =HBaseConnection.getHBaseConnection().getAdmin();

//        表描述器
        HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));

//        循环添加列族
        Arrays.asList(cfs).forEach(cf -> {
            HColumnDescriptor columnDescriptor = new HColumnDescriptor(cf);
            columnDescriptor.setMaxVersions(10);
            tableDescriptor.addFamily(columnDescriptor);
        });


        admin.createTable(tableDescriptor);

//        关闭资源
        admin.close();
        HBaseConnection.closeConnection();

    }

    /**
     *  发布微博
     *  1.更新微博内容表数据
     *  2.更新收件箱表数据
     *      --获取当前操作人的粉丝
     *      --去往收件箱表依次更新数据
     */
    public static void createData(String uid ,String content) throws IOException {

//        获取表对象
        Table contentTable = HBaseConnection.getTable(Constant.CONTENT);
        Table relationsTable =HBaseConnection.getTable(Constant.RELATIONS);
        Table inboxTable = HBaseConnection.getTable(Constant.INBOX);

        long timeMillis = System.currentTimeMillis();

//        rowKey 下划线分割
        String rowKey = uid + "_" + timeMillis;

        Put put = new Put(Bytes.toBytes(rowKey));

        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("content"), Bytes.toBytes(content));

//        往内容表添加数据
        contentTable.put(put);

        Get get = new Get(Bytes.toBytes(uid));
//        获取关系表中粉丝
        get.addFamily(Bytes.toBytes("fans"));
        Result result = relationsTable.get(get);

//        单元格
        Cell[] cells = result.rawCells();
        if (cells.length <= 0){
            return;
        }

        List<Put> puts = new ArrayList<>();
        for (Cell cell : cells) {
            byte[] qualifier = CellUtil.cloneQualifier(cell);

//            更新粉丝收件箱
            Put inboxPut = new Put(qualifier);
            inboxPut.addColumn(Bytes.toBytes("info"), Bytes.toBytes(uid), Bytes.toBytes(rowKey));
            puts.add(inboxPut);
        }

//        更新粉丝收件箱
        inboxTable.put(puts);

        inboxTable.close();
        relationsTable.close();
        contentTable.close();
        HBaseConnection.closeConnection();
    }

    /**
     *  关注用户
     *  1.在用户关系表
     *      --添加操作人的 attends
     *      --添加被操作人的 fans
     *
     *  2.在收件箱表
     *      --在微博内容中获取被关注的 3 条数据 rowKey
     *      --在收件箱表中添加操作人的关注者信息
     */
    public static void addAttend(String uid ,String... uids) throws IOException {
//        获取表对象
        Table contentTable = HBaseConnection.getTable(Constant.CONTENT);
        Table relationsTable =HBaseConnection.getTable(Constant.RELATIONS);
        Table inboxTable = HBaseConnection.getTable(Constant.INBOX);

//        创建操作者的 put 对象
        Put relaPut = new Put(Bytes.toBytes(uid));

        List<Put> puts = new ArrayList<>();

        for (String s : uids){
            relaPut.addColumn(Bytes.toBytes("attends"), Bytes.toBytes(s), Bytes.toBytes(s));
//            创建被关注者的 put 对象 添加粉丝
            Put fansPut = new Put(Bytes.toBytes(s));
            fansPut.addColumn(Bytes.toBytes("fans"), Bytes.toBytes(uid), Bytes.toBytes(uid));
            puts.add(fansPut);
        }

        puts.add(relaPut);

        relationsTable.put(puts);

        Put inboxPut = new Put(Bytes.toBytes(uid));
//        操作收件箱表
//        获取内容表中被关注者的 rowKey
        for (String s : uids) {
            /**
             * |比_ 大
             * rowKey : 开始rowKey  uid_   结束stopRow  uid|
             * @param startRow row to start scanner at or after (inclusive)
             * @param stopRow row to stop scanner before (exclusive)
             */
            Scan scan = new Scan(Bytes.toBytes(s),Bytes.toBytes(s + "|"));
            ResultScanner results = contentTable.getScanner(scan);
            for (Result result : results){
                String rowKey = Bytes.toString( result.getRow());
                String[] split = rowKey.split("_");
                byte[] row = result.getRow();
//                使用发布微博的时间戳
                inboxPut.addColumn(Bytes.toBytes("info"), Bytes.toBytes(s), Long.parseLong(split[1]) , row);
            }
        }
        inboxTable.put(inboxPut);

        inboxTable.close();
        relationsTable.close();
        contentTable.close();
        HBaseConnection.closeConnection();
    }

    /**
     *  取关用户
     *  操作用户关系表 删除操作者关注列族的待取关用户
     *  删除待取关用户粉丝列族下的操作者
     *
     *  收件箱
     *      删除操作者的待取关用户的信息
     */
    public static void delAttend(String uid ,String... uids) throws IOException {

//        获取表对象
        Table relationsTable =HBaseConnection.getTable(Constant.RELATIONS);
        Table inboxTable = HBaseConnection.getTable(Constant.INBOX);

        Delete relaDel = new Delete(Bytes.toBytes(uid));

//        批量操作
        List<Delete> deletes = new ArrayList<>();

        for (String s : uids){
            Delete fansDel = new Delete(Bytes.toBytes(s));
            fansDel.addColumn(Bytes.toBytes("fans"), Bytes.toBytes(uid));
            relaDel.addColumn(Bytes.toBytes("attends"), Bytes.toBytes(s));
            deletes.add(fansDel);
        }
        deletes.add(relaDel);

        relationsTable.delete(deletes);

//        删除收件箱
        Delete inboxDel = new Delete(Bytes.toBytes(uid));
        for (String s : uids){
            inboxDel.addColumn(Bytes.toBytes("info"), Bytes.toBytes(s));
        }

//        执行收件箱表的删除操作
        inboxTable.delete(inboxDel);

        inboxTable.close();
        relationsTable.close();
        HBaseConnection.closeConnection();
    }

    /**
     *  获取微博内容 (初始化页面)
     *  当前用户 id
     */
    public static void getInit(String uid) throws IOException {
//        获取表对象
        Table contentTable = HBaseConnection.getTable(Constant.CONTENT);
        Table inboxTable = HBaseConnection.getTable(Constant.INBOX);

//        获取收件箱的数据
        Get get = new Get(Bytes.toBytes(uid));

//        设置 2  每次获取的是最新的 2 条数据
        get.setMaxVersions(2);

//        获取数据
        Result result = inboxTable.get(get);

        Cell[] cells = result.rawCells();

        List<Get> gets = new ArrayList<>();

//        遍历返回内容 并封装成内容表的 get 对象
        for (Cell cell : cells) {
            Get contGet = new Get(CellUtil.cloneValue(cell));
            gets.add(contGet);
        }

//        根据收件箱获取的值去往内容表获取微博内容
        Result[] results = contentTable.get(gets);
        for (Result result1 : results) {
            Cell[] cells1 = result1.rawCells();

            for (Cell cell : cells1){
                System.out.println("pk = " + Bytes.toString(CellUtil.cloneRow(cell))
                        + ",Content: " + Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }
        contentTable.close();
        inboxTable.close();
        HBaseConnection.closeConnection();
    }


    /**
     *  获取微博内容 (查看某个人的所有微博)
     */
    public static void getData(String uid) throws IOException {
        Table contentTable = HBaseConnection.getTable(Constant.CONTENT);
        Scan scan = new Scan();

//        通过过滤器
        RowFilter rowFilter = new RowFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator(uid + "_"));

        scan.setFilter(rowFilter);

        ResultScanner scanner = contentTable.getScanner(scan);

        for (Result result : scanner){
            Cell[] cells = result.rawCells();
            for (Cell cell : cells){
                System.out.println("pk = " + Bytes.toString(CellUtil.cloneRow(cell))
                        + ",Content: " + Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }
        contentTable.close();
        HBaseConnection.closeConnection();
    }
}

