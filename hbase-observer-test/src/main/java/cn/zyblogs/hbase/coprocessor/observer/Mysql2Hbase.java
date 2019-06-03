package cn.zyblogs.hbase.coprocessor.observer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * @author Yibo Zhang
 * @date 2019/06/03
 */
public class Mysql2Hbase {

    public static class Mysql2HbaseMapper extends Mapper<LongWritable,StuHbase,StuHbase, NullWritable>{
        @Override
        protected void map(LongWritable key, StuHbase value, Context context) throws IOException, InterruptedException {
            System.err.println("******************");
            System.out.println(value);
            context.write(value,NullWritable.get());
        }
    }

    public static class Mysql2HbaseReducer extends TableReducer<StuHbase,NullWritable,ImmutableBytesWritable> {
        //
        @Override
        protected void reduce(StuHbase key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            final Put put = new Put(Bytes.toBytes("03_001"));

            for(NullWritable v : values){
                put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("name"),Bytes.toBytes(key.getName()));
                //此处+ “” 的目的是 为了不再 hbase中 显示乱码， 先把数字转为字符串
                put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("age"),Bytes.toBytes(key.getAge()+""));
                put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("sex"),Bytes.toBytes(key.getSex()));
                put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("grade"),Bytes.toBytes(key.getGrade()+""));
            }

            //此处要传入的是（ImmutableBytesWritable, Mutation） ,put是mutation的子类
            context.write(/*new ImmutableBytesWritable(*//*Bytes.toBytes("02_001"))*/new ImmutableBytesWritable(Bytes.toBytes("03_001")),put);
            //context.write();
        }
    }

    public static class Mysql2HbaseDriver extends Configured implements Tool{

        public static void main(String[] args) throws Exception {
            Configuration conf = HBaseConfiguration.create();
            //设置连接的zookeeper的地址，可以对hbase进行操作
            conf.set("hbase.zookeeper.quorum","hix2-virtual-machine:2181");
            ToolRunner.run(conf,new Mysql2HbaseDriver(),args);
        }

        @Override
        public int run(String[] strings) throws Exception {
            Configuration conf = this.getConf();
            //配置MySQL的的url,用户名和密码
            DBConfiguration.configureDB(conf,"com.mysql.jdbc.Driver","jdbc:mysql://101.132.175.168:3306/test","root","123456");

            final Job job = Job.getInstance(conf);

            job.setJarByClass(Mysql2HbaseDriver.class);

            job.setMapperClass(Mysql2HbaseMapper.class);

            job.setMapOutputKeyClass(StuHbase.class);
            job.setMapOutputValueClass(NullWritable.class);
            //要把数据存储的hbase中的stu1表
            TableMapReduceUtil.initTableReducerJob("stu1",Mysql2HbaseReducer.class,job);

            //设置输入格式是从Database中读取
            job.setInputFormatClass(DBInputFormat.class);
            // job,继承DBWritable的类，表名，查询条件，按那个字段进行排序，要读取的字段
            DBInputFormat.setInput(job,StuHbase.class,"stu",null,"grade","name","age","sex","grade");

            job.waitForCompletion(true);
            return 0;
        }
    }
}
