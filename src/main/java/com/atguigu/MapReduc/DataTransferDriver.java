package com.atguigu.MapReduc;


import com.atguigu.HdfsDataHbas.ReadFruitFromHDFSMapper;
import com.atguigu.HdfsDataHbas.WriteFruitMRFromTxtReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;


public class DataTransferDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = HBaseConfiguration.create();

        Job job = Job.getInstance(configuration);

        job.setJobName("hbasemr2");

        job.setJarByClass(DataTransferDriver.class);

        job.setMapperClass(ReadFruitFromHDFSMapper.class);

        job.setMapOutputKeyClass(ImmutableBytesWritable.class);

        job.setMapOutputValueClass(Put.class);

        FileInputFormat.setInputPaths(job,new Path("hdfs://hadoop101:9000/fruit.tsv"));

        TableMapReduceUtil.initTableReducerJob("fruit2", WriteFruitMRFromTxtReducer.class, job);

        job.waitForCompletion(true);
    }
}