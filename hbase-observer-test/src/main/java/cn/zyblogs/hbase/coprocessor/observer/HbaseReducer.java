package cn.zyblogs.hbase.coprocessor.observer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class HbaseReducer extends Reducer<Text,IntWritable,StuHbase,Text>{
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sumGrade = 0;//记录 总分
            for(IntWritable v : values){
                sumGrade = v.get() +sumGrade;
            }

        final String[] split = key.toString().split("_"); //把传过来的name_age_sex进行切割
        //进行赋值
        final String name = split[0]; 
        int age = Integer.parseInt(split[1]);
        String sex = split[2];
        //把想要存到数据的值赋给自定一个继承DBWritable的类，value置为null
        context.write(new StuHbase(name,age,sex,sumGrade),null);
    }
}