package cn.zyblogs.hbase.coprocessor.observer;


import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * @author Yibo Zhang
 * @date 2019/05/05
 */
public class RegionObserverTest extends BaseRegionObserver {

    private byte[] columnFamily = Bytes.toBytes("cf");
    private byte[] countCol = Bytes.toBytes("countCol");
    private byte[] unDeleteCol = Bytes.toBytes("unDeleteCol");
    private RegionCoprocessorEnvironment environment;

    /**
     *  RegionServer 打开 Region 前执行
     *
     * @param e     CoprocessorEnvironment
     * @throws IOException  IOException
     */
    @Override
    public void start(CoprocessorEnvironment e) throws IOException {
        environment = (RegionCoprocessorEnvironment) e;
    }

    /**
     * RegionServer 打开 Region 前执行
     *
     * @param e     CoprocessorEnvironment
     * @throws IOException  IOException
     */
    @Override
    public void stop(CoprocessorEnvironment e) throws IOException {

    }

    /**
     *  1. cf :countCol 进行累加操作 每次插入的时候都要与之前的值进行相加
     */
    @Override
    public void prePut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability) throws IOException {
        if (put.has(columnFamily, countCol)){
//            获取 old  countCol 累加
            Result result = e.getEnvironment().getRegion().get(new Get(put.getRow()));
            Cell[] rawCells = result.rawCells();
            int oldNum = 0;
            for (Cell cell : rawCells) {
                if (CellUtil.matchingColumn(cell, columnFamily, countCol)) {
                    oldNum = Integer.valueOf(Bytes.toString(CellUtil.cloneValue(cell)));
                }
            }

//            获取 new countCol
            List<Cell> cells = put.get(columnFamily, countCol);
            int newNum = 0;
            for (Cell cell : cells) {
                if (CellUtil.matchingColumn(cell, columnFamily, countCol)) {
                    newNum = Integer.valueOf(Bytes.toString(CellUtil.cloneValue(cell)));
                }
            }

//            sum AND update Put实例
            put.addColumn(columnFamily, countCol, Bytes.toBytes(String.valueOf(oldNum + newNum)));

        }
    }

    /**
     *   不能直接删除unDeleteCol    删除countCol的时候将unDeleteCol一同删除
     */
    @Override
    public void preDelete(ObserverContext<RegionCoprocessorEnvironment> e, Delete delete, WALEdit edit, Durability durability) throws IOException {
//        判断是否操作 cf 列族
        List<Cell> cells = delete.getFamilyCellMap().get(columnFamily);
        if (cells != null && cells.size() > 0) {
            boolean deleteFlag = false;
            for (Cell cell : cells) {
                byte[] qualifier = CellUtil.cloneQualifier(cell);
                if (Arrays.equals(qualifier, unDeleteCol)){
                    throw new IOException("can not delete unDel column");
                }

                if (Arrays.equals(qualifier, countCol)){
                    deleteFlag = true;
                }
            }
            if (deleteFlag){
                delete.addColumn(columnFamily, unDeleteCol);
            }
        }

    }
}
