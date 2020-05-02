package com.atguigu.HdfsDataHbas;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;


public class Txt2FruitRunner {
    public static  void  main(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();

        Job job = Job.getInstance(conf);

        job.setJobName("hbasemr12");

        job.setJarByClass(Txt2FruitRunner.class);

        job.setMapperClass(ReadFruitFromHDFSMapper.class);

        job.setMapOutputKeyClass(ImmutableBytesWritable.class);

        job.setMapOutputValueClass(Put.class);

        //hdfs://hadoop101:9000/movie hdfs地址
        FileInputFormat.setInputPaths(job, new Path("hdfs://hadoop101:9000/fruit.tsv"));

        //movie 为Hbase数据库
        TableMapReduceUtil.initTableReducerJob("fruit2", WriteFruitMRFromTxtReducer.class, job);

        job.waitForCompletion(true);

    }

}