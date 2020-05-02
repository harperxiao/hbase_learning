package com.atguigu.Api;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import javax.print.DocFlavor;
import java.io.IOException;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.List;

public class HBaseJavaApiWord {

    /*   Connection可以通过ConnectionFactory类实例化。
    Connection的生命周期由调用者管理，使用完毕后需要执行close()以释放资源。
    Connection是线程安全的，多个Table和Admin可以共用同一个Connection对象。因此一个客户端只需要实例化一个连接即可。
    反之，Table和Admin不是线程安全的！因此不建议并缓存或池化这两种对象。
    */
    public static Configuration conf;

    static {
        //使用HBaseConfiguration 的单例方法实例化
        conf = HBaseConfiguration.create();
    }

    /*Admin为HBase的管理类，可以通过Connection.getAdmin（）获取实例，且在使用完成后调用close（）关闭。
    Admin可用于创建，删除，列出，启用和禁用以及以其他方式修改表，以及执行其他管理操作。
    */
    //判断表是否存在
    public static boolean isTableExist(String tableName) throws MasterNotRunningException,
            ZooKeeperConnectionException, IOException {
        //在HBase中管理、访问表需要先创建HBaseAdmin对象
        //Connection connection = ConnectionFactory.createConnection(conf);
        //HBaseAdmin admin = (HBaseAdmin) connection.getAdmin();
        HBaseAdmin admin = new HBaseAdmin(conf);
        return admin.tableExists(tableName);
    }

    //创建表
    public static boolean createTable (String nsname,String tableName,String ...columnFamily) throws IOException {
        TableName tn =null;
        HBaseAdmin admin = new HBaseAdmin(conf);
        //判断表是否存在
        if (isTableExist(tableName)){
            System.out.println("表"+tableName+"已经存在");
            System.exit(1);//正常退出，关闭虚拟机   0：为非正常退出
        }else {
            if (StringUtils.isEmpty(nsname)){
                tn=TableName.valueOf(tableName);
            }else {
                tn=TableName.valueOf(nsname,tableName);
            }
        }

        try {
            //创建表属性对象，表名需要转字节
            HTableDescriptor descriptor = new HTableDescriptor(tn);

            //创建多个列族
            for (String cf : columnFamily) {
                descriptor.addFamily(new HColumnDescriptor(cf));
            }

            //根据对表的配置，创建表
            admin.createTable(descriptor);
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
        System.out.println("表"+tableName+"创建成功！！！");
        return true;
    }

    //删除表
    public static boolean dropTable(String tableName) throws IOException {
        HBaseAdmin admin = new HBaseAdmin(conf);
        if (isTableExist(tableName)){
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
            System.out.println("表"+tableName+"删除成功");
            return true;
        }else {
            System.out.println("表"+tableName+"不存在！");
            return false;
        }
    }

    //向表中插入数据
    public static boolean addRowData(String tablename, String rowKey,String columnFamily,String column
        ,String value) throws IOException {
        //创建表对象
        try {
            HTable hTable = new HTable(conf,tablename);
            //向表中插入数据
            Put put = new Put(Bytes.toBytes(rowKey));
            //向put对象中组装数据
            put.add(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(value));
            hTable.put(put);
            hTable.close();
            System.out.println("插入数据成功");
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    //删除多行数据
    public static boolean deleteMultiTRow(String tableName,String ...rows) throws IOException {
        //获取表
        HTable hTable = new HTable(conf,tableName);

        List<Delete> deleteList = new ArrayList<Delete>();

        for (String row:rows){
            Delete delete = new Delete(Bytes.toBytes(row));
            deleteList.add(delete);
        }
        hTable.delete(deleteList);
        hTable.close();
        return true;
    }

    //获取所有数据
    public static boolean getAllRows(String tableName) throws IOException {
        try {
            HTable hTable =new HTable(conf,tableName);
            //得到用于扫描region的对象
            Scan scan = new Scan();
            //使用HTable 得到 resultcanner 实现类的对象
            ResultScanner resultScanner =hTable.getScanner(scan);
            for(Result result : resultScanner){
                Cell[] cells = result.rawCells();
                for(Cell cell : cells){
                    System.out.println("列族" + Bytes.toString(CellUtil.cloneFamily(cell))+"行键:" + Bytes.toString(CellUtil.cloneRow(cell))
                    +"列:" + Bytes.toString(CellUtil.cloneQualifier(cell))+"值:" + Bytes.toString(CellUtil.cloneValue(cell)));
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
            return true;
        }
        return false;
    }

    public static void main(String[] args) throws IOException {
       // System.out.println(isTableExist("fruit2"));
        //String[] arr ={"info","info1","info2"};
        //System.out.println(createTable(null,"h10",arr));
       // System.out.println(dropTable("12"));
        System.out.println(getAllRows("hive_hbase_emp_table"));
        // System.out.println(111);
    }
}