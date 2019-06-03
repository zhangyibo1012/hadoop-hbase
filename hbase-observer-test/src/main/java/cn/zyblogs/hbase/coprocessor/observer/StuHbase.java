package cn.zyblogs.hbase.coprocessor.observer;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @author Yibo Zhang
 * @date 2019/06/03
 */
public class StuHbase implements WritableComparable<StuHbase>, DBWritable {

    //与mysql中表对应的
    private  String name;
    private  int age;
    private  String sex;
    private  int grade;

    public StuHbase(){}
    public StuHbase(String name,int age,String sex,int grade){
        this.name = name;
        this.age = age;
        this.sex = sex;
        this.grade = grade;
    }

    //这里写和读的字段的顺序要一样
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(name);
        dataOutput.writeInt(age);
        dataOutput.writeUTF(sex);
        dataOutput.writeInt(grade);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.name = dataInput.readUTF();
        this.age = dataInput.readInt();
        this.sex = dataInput.readUTF();
        this.grade = dataInput.readInt();
    }

    @Override
    public void write(PreparedStatement preparedStatement) throws SQLException {
        //类似于jdbc是使用preparedStatement，进行赋值
        int index = 1;
        preparedStatement.setString(index++,name);
        preparedStatement.setInt(index++,age);
        preparedStatement.setString(index++,sex);
        preparedStatement.setInt(index,grade);
    }

    @Override
    public void readFields(ResultSet resultSet) throws SQLException {
        //类似于jdbc进行查询
        int index = 1;
        name = resultSet.getString(index++);
        age = resultSet.getInt(index++);
        sex = resultSet.getString(index++);
        grade = resultSet.getInt(index);
    }

    public String getName() {
        return name;
    }

    public int getAge() {
        return age;
    }

    public String getSex() {
        return sex;
    }

    public int getGrade() {
        return grade;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public void setSex(String sex) {
        this.sex = sex;
    }

    public void setGrade(int grade) {
        this.grade = grade;
    }

    @Override
    public String toString() {
        return name+"\t"+age+"\t"+sex+"\t"+grade;
    }

    @Override
    public int compareTo(StuHbase o) {
        return this.grade-o.grade;
    }
}
