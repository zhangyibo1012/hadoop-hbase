package cn.zyblogs.hbase.api.weibo;

import javax.xml.ws.WebEndpoint;
import java.io.IOException;

/**
 * @author Yibo Zhang
 * @date 2019/05/14
 */
public class Weibo {

    public static void init() throws IOException {
//        创建相关命名空间和表
        WeiboUtil.createNamespace(Constant.NAMESPACES);
        WeiboUtil.createTable(Constant.CONTENT, 1,"info");
        WeiboUtil.createTable(Constant.RELATIONS, 1,"attends" ,"fans");
        WeiboUtil.createTable(Constant.INBOX, 2,"info");

    }

    public static void main(String[] args) throws IOException {
//        init();

//        1001 1002 发布微博 scan 'weibo:content'
//        WeiboUtil.createData("1001", "我是1001");
//        WeiboUtil.createData("1002", "我是1002");

        /**
         *  1001 关注 1002 和 1003
         */
//        WeiboUtil.addAttend("1001","1002" , "1003");

//        获取 1001 初始化信息
//        WeiboUtil.getInit("1001");

//        1003 发布微博
//        WeiboUtil.createData("1003", "第二次 +");
//        WeiboUtil.createData("1003", "n +");
//        获取 1001 初始化信息
        WeiboUtil.getInit("1001");

//        取关
        WeiboUtil.delAttend("1001","1002");

        System.out.println();

        WeiboUtil.getInit("1001");
    }

}
