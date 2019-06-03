package cn.zyblogs.hbase.coprocessor.observer;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class HBaseMap  extends TableMapper<Text,IntWritable>{
    /**
     *这个MapReduce是简单实现对求 每个人的总分数
     * @param key rowKey, 在hbase中设计的学科号_学号
     * @param value cell的集合
     * @param context 上下文
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        Cell[] cells = value.rawCells();
        String name=null;
        int age = 0;
        String sex = null;
        int grade = 0;
        for(Cell cell : cells){
            //获取列名
            String clomun = new String(CellUtil.cloneQualifier(cell));
            //获取cell的值，就是这一列中的值
            String v = new String(CellUtil.cloneValue(cell));

            switch(clomun){  //根据列名赋予给相应的值
                case "name":
                    name = v;
                    break;
                case "age":
                    age = Integer.parseInt(v);
                    break;
                case "sex":
                    sex = v;
                    break;
                case "grade":
                    grade = Integer.parseInt(v);
                    break;
            }

        }
        // 把值写入
        context.write(new Text(name+"_"+age+"_"+sex),new IntWritable(grade));
    }
}