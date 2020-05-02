package com.atguigu.Api;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

/*
 * Admin: 提供表管理的客户端，可以从Connection.getAdmin(),使用完毕需要执行close()关闭！
 * 			可以创建表，删除表，...
 *
 * 			保证一个线程拥有一个自己的admin对象
 *
 * Connection: 代表对集群的连接，负责创建Admin和Table.一个Connection可以在多个Admin和Table共享。
 * 				调用者需要自己执行close()释放连接。
 *
 * 				一个应用只需要创建一个Connection，是线程安全的！
 * 				每个线程需要创建自己的Admin和Table！
 *
 * HBase存储的所有数据的参数都是byte[],hbase提供了一个工具类Bytes,有toBytes()和toxxx()方法，方便进行byte[]和其他类型的转换！
 *
 *
 * HBase提供CellUtil来针对cell的操作，通常使用CellUtil.clonexxx();
 *
 */
public class HBaseJavaApi {

    // hbase shell: 创建一个客户端
    public static Connection conn;

    // ThreadLocal拥有一个map，map集合中的key为当前线程，value为保存的数据
    // 保证每个线程都拥有一个自己的数据
    public static ThreadLocal<Admin> adminTL;

    public static ThreadLocal<Map<String, Table>> tableTL;

    static {

        try {

            // HBaseConfiguration.create()：创建一个configuration，读取hbase的配置文件！
            conn = ConnectionFactory.createConnection();

            adminTL = new ThreadLocal<>();

            tableTL = new ThreadLocal<>();

            tableTL.set(new HashMap<>());

        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }

    // 获取一个Table
    public static Table getTable(String nsname, String tablename) throws IOException {

        if (!ifTableExists(nsname, tablename)) {

            System.err.println("当前表不存在！");

            return null;

        }

        Map<String, Table> tableMap = tableTL.get();

        // 声明表名
        TableName tn = null;

        if (StringUtils.isEmpty(nsname)) {

            tn = TableName.valueOf(tablename);

        } else {

            tn = TableName.valueOf(nsname, tablename);

        }

        Table table = tableMap.get(Bytes.toString(tn.getName()));

        // 判断table是否之前已经创建
        if (table == null) {

            table = conn.getTable(tn);

            tableMap.put(Bytes.toString(tn.getName()), table);

        }

        return table;

    }

    // put操作:  put '表名'，'rowkey','列族：列名', value, [ts]
    public static void putData(String nsname, String tablename, String rowkey, String family, String column, String value) throws IOException {

        // 获取table对象
        Table table = getTable(nsname, tablename);

        // 一个put对象，就是添加一行数据
        Put put = new Put(Bytes.toBytes(rowkey));

        // 向put添加单元格 cell(为一行中的列复制)
        put.addColumn(Bytes.toBytes(family), Bytes.toBytes(column), Bytes.toBytes(value));
        //.addColumn(family, qualifier, value);
        table.put(put);


    }

    // delete操作  put '表名'，'rowkey'
    public static void deleteData(String nsname, String tablename, String rowkey) throws IOException {

        // 获取table对象
        Table table = getTable(nsname, tablename);

        // 删除某行
        Delete delete = new Delete(Bytes.toBytes(rowkey));

        // 删除指定列族
        //delete.addFamily(Bytes.toBytes("info"));

        // 删除指定列:  标记type=Delete ，时间戳是指定记录的时间戳，只删除单条记录！
        delete.addColumn(Bytes.toBytes("info"), Bytes.toBytes("gender"));

        // 删除指定列的指定版本cell
        //delete.addColumn(family, qualifier, timestamp)

        // 指定到rowkey，删除整行的所有列族，添加一列记录： DeleteFamily
        table.delete(delete);

    }

    // get 操作： 查询单行  get '表名','rk',['']
    public static Result getData(String nsname, String tablename, String rowkey) throws IOException {

        // 获取table对象
        Table table = getTable(nsname, tablename);
        // 一行的get对象
        Get get = new Get(Bytes.toBytes(rowkey));
        //指定列
        get.addColumn(Bytes.toBytes("info"), Bytes.toBytes("gender"));
        // 查询某个列族
        //get.addFamily(family)

        // 设置获取多少个版本的cell，设置获取和列族的VERSIONS属性一致的cell
        get.setMaxVersions(2);

        // 代表当行记录的value
        Result result = table.get(get);

        return result;

    }

    // scan 操作： 查询多行
    public static ResultScanner scanTable(String nsname, String tablename) throws IOException {

        // 获取table对象
        Table table = getTable(nsname, tablename);

        Scan scan = new Scan();

        // 扫描指定列
        //scan.addColumn(family, qualifier)

        // 扫描指定列族
        //scan.addFamily(family)

        scan.setStartRow(Bytes.toBytes("r3"));

        scan.setStopRow(Bytes.toBytes("r6"));

        scan.setMaxVersions();

        scan.setRaw(true);

        ResultScanner scanner = table.getScanner(scan);

        return scanner;

    }



    // 返回一个Admin客户端
    public static Admin getAdmin() throws IOException {

        Admin admin = adminTL.get();

        if (admin == null) {

            admin = conn.getAdmin();

            adminTL.set(admin);

        }

        return admin;

    }

    //表操作： 建表
    public static boolean createTable(String nsname, String tablename, String... cfs) throws IOException {

        if (ifTableExists(nsname, tablename) || cfs == null) {

            System.err.println("表已经存在或者请输入至少一个列族！");

            return false;
        }

        Admin admin = getAdmin();

        // 声明表名
        TableName tn = null;

        if (StringUtils.isEmpty(nsname)) {

            tn = TableName.valueOf(tablename);

        } else {

            tn = TableName.valueOf(nsname, tablename);

        }

        // 表的所有属性必须在HTableDescriptor中声明
        HTableDescriptor htabledesc = new HTableDescriptor(tn);

        for (String cf : cfs) {

            HColumnDescriptor hcfdesc = new HColumnDescriptor(cf);

            // 添加表的属性
            htabledesc.addFamily(hcfdesc);
        }

        try {

            admin.createTable(htabledesc);

        } catch (Exception e) {
            e.printStackTrace();

            return false;
        }

        return true;

    }

    //表操作： 判断表是否存在
    public static boolean ifTableExists(String nsname, String tablename) throws IOException {

        if (StringUtils.isEmpty(tablename)) {

            System.err.println("tablename不能为null！");

            return false;

        }

        Admin admin = getAdmin();

        TableName tn = null;


        if (StringUtils.isEmpty(nsname)) {

            tn = TableName.valueOf(tablename);

        } else {

            tn = TableName.valueOf(nsname, tablename);

        }

        return admin.tableExists(tn);

    }

    //表操作： 删除表
    public static boolean deleteTable(String nsname, String tablename) throws IOException {

        if (!ifTableExists(nsname, tablename)) {

            System.err.println("当前表不存在！");

            return false;
        }

        Admin admin = getAdmin();

        // 声明表名
        TableName tn = null;

        if (StringUtils.isEmpty(nsname)) {

            tn = TableName.valueOf(tablename);

        } else {

            tn = TableName.valueOf(nsname, tablename);

        }

        try {
            // 先禁用表
            admin.disableTable(tn);

            // 删除表
            admin.deleteTable(tn);
        } catch (Exception e) {
            e.printStackTrace();

            return false;
        }

        return true;

    }

    // 库操作
    // DDL: 建namespace
    public static boolean createNameSpace(String nsname) throws IOException {

        if (StringUtils.isEmpty(nsname)) {

            System.err.println("nsname不能为null！");

            return false;

        }

        if (isExistsNameSpace(nsname)) {

            System.err.println("nsname已经存在！");

            return false;

        }

        Admin admin = getAdmin();

        NamespaceDescriptor nsdesc = NamespaceDescriptor.create(nsname).build();

        admin.createNamespace(nsdesc);

        return true;

    }

    // DDL: 列出所有的namespace
    public static void listNameSpace() throws IOException {

        Admin admin = getAdmin();

        NamespaceDescriptor[] nsdesc = admin.listNamespaceDescriptors();

        for (NamespaceDescriptor namespaceDescriptor : nsdesc) {

            System.out.println(namespaceDescriptor);

        }


    }

    // DDL: 删除namespace
    public static boolean deleleNameSpace(String nsname) throws IOException {

        if (StringUtils.isEmpty(nsname)) {

            System.err.println("nsname不能为null！");

            return false;

        }

        if (!isExistsNameSpace(nsname)) {

            System.err.println("nsname不存在！");

            return false;

        }

        try {
            Admin admin = getAdmin();

            admin.deleteNamespace(nsname);

        } catch (Exception e) {

            e.printStackTrace();

            return false;
        }

        return true;

    }


    // DDL: 判断是否存在namespace
    public static boolean isExistsNameSpace(String nsname) throws IOException {

        Admin admin = getAdmin();

        NamespaceDescriptor nsdesc = null;
        try {
            nsdesc = admin.getNamespaceDescriptor(nsname);

            return true;

        } catch (Exception e) {

            return false;
        }


    }

    // 释放资源
    public static void close() throws IOException {

        //关闭admin
        Admin admin = adminTL.get();

        if (admin != null) {

            admin.close();

            adminTL.remove();

        }

        //关闭table
        Map<String, Table> map = tableTL.get();

        Collection<Table> tables = map.values();

        for (Table table : tables) {

            table.close();

        }

        tableTL.remove();

        if (conn != null) {

            conn.close();
        }


    }

    public static void main(String[] args) throws Exception {

        //System.out.println(conn);

        //System.out.println(createNameSpace("ns2"));

        //System.out.println(deleleNameSpace("ns2"));

		/*System.out.println(ifTableExists(null, "t2"));
		System.out.println(ifTableExists("hbase", "t2"));
		System.out.println(ifTableExists("ns1", "t2"));
		System.out.println(ifTableExists(null, "t1"));
		System.out.println(ifTableExists(null, null));*/

        //System.out.println(createTable("ns2", "t2", null));
        //	System.out.println(createTable(null, "t2", ""));
        //System.out.println(createTable(null, "t2", "info","info1"));

        //System.out.println(deleteTable(null, "t2"));
        //System.out.println(deleteTable(null, null));
        //System.out.println(deleteTable(null, "t2"));

        //putData(null, "t1", "r6", "info", "haha", "haha");

        //Result result = getData(null, "t1", "r3");

		/*ResultScanner scanner = scanTable(null, "t1");

		for (Result result : scanner) {

			Cell[] cells = result.rawCells();

			for (Cell cell : cells) {

				System.out.println("rowkey:"+Bytes.toString(CellUtil.cloneRow(cell))
				+"===》family:"+Bytes.toString(CellUtil.cloneFamily(cell))
				+"===》column:"+Bytes.toString(CellUtil.cloneQualifier(cell))
				+"===》value:"+Bytes.toString(CellUtil.cloneValue(cell))
						);

			}
		}*/

        deleteData(null, "t1", "r4");


        //释放资源
        close();

    }


}
