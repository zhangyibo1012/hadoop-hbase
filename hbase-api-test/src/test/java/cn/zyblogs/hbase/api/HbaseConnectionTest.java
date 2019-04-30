package cn.zyblogs.hbase.api;

import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Table;
import org.junit.Test;

/**
 * @author Yibo Zhang
 * @date 2019/04/30
 */
public class HbaseConnectionTest {

    /**
     *  测试链接
     */
    @Test
    public void getConnectionTest(){
        Connection hBaseConnection = HBaseConnection.getHBaseConnection();
        System.out.println("hBaseConnection = " + hBaseConnection);
        HBaseConnection.closeConnection();
        System.out.println(hBaseConnection.isClosed());
    }

    /**
     *  测试获取 Table 实例
     */
    @Test
    public void getTableTest(){
        try {
            Table table = HBaseConnection.getTable("US_POPULATION");
            System.out.println("table = " + table.getName().getNameAsString());
            table.close();
        }catch (Exception ex){
            ex.printStackTrace();
        }
    }
}
