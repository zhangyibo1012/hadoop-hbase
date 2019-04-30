package cn.zyblogs.hbase.api;

import org.junit.Test;

import java.io.IOException;

/**
 * @author Yibo Zhang
 * @date 2019/04/30
 */
public class HBaseUtilTest {

    @Test
    public void createTable() throws IOException {
        HBaseUtil.createTable("hixTable", new String[]{"brocastInfo", "saveInfo"});
        System.out.println("HBaseUtilTest.createTable ok ");
    }
}
