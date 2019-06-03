package cn.zyblogs.hbase.coprocessor.observer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author Yibo Zhang
 * @date 2019/06/03
 */
public class StuHbaseDriver extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        final Configuration conf = HBaseConfiguration.create();
        //想要对hbase进行操作，需要连接zookeeper，不管是读还是写，都是先从zookeeper中获取元数据信息
        conf.set("hbase.zookeeper.quorum","hix2-virtual-machine:2181");

        ToolRunner.run(conf,new StuHbaseDriver(),args);
    }

    @Override
    public int run(String[] strings) throws Exception {

        Configuration conf = this.getConf();

//        设定要写入的mysql的url和用户名和密码
        DBConfiguration.configureDB(conf,"com.mysql.jdbc.Driver","jdbc:mysql://101.132.175.168:3306/test","root","123456");

        Job job = Job.getInstance(conf);
        job.setJarByClass(StuHbaseDriver.class);

        Scan scan = new Scan();
        scan.setCacheBlocks(false);
        scan.setCaching(500);

        //设置map，表名，scan，Map类.class,输出的key,输出的value，job
        TableMapReduceUtil.initTableMapperJob("stu1",scan,HBaseMap.class,Text.class, IntWritable.class,job);

//设置reduce的类
        job.setReducerClass(HbaseReducer.class);
        //设置输出格式是DataBase
        job.setOutputFormatClass(DBOutputFormat.class);
//设置输出时的k,v类型
        job.setOutputKeyClass(StuHbase.class);
        job.setOutputValueClass(Text.class);

        //设置job 输出到mysql时 的 表名，和对应的列
        DBOutputFormat.setOutput(job,"stu","name","age","sex","grade");

        job.waitForCompletion(true);

        return 0;
    }
}
