package com.riskiq.mapreduce.io;

import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;
import com.google.common.collect.Multisets;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * This input format will create a split per file found in the configured input directories.
 * The resulting map input keys will be {@link IntWritable}s (the file's partition ID if present), and the input value
 * will be a {@link Text} containing the file's absolute path.
 * @author ahunt
 */
public class FilenameInputFormat extends FileInputFormat<IntWritable, Text> {

    private static final Log LOG = LogFactory.getLog(FilenameInputFormat.class);

    @Override
    public RecordReader<IntWritable, Text> createRecordReader(InputSplit genericSplit, TaskAttemptContext context) throws IOException, InterruptedException {
        context.setStatus(genericSplit.toString());
        return new FilenameRecordReader();
    }


    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException {
        Configuration conf = context.getConfiguration();

        List<InputSplit> splits = new ArrayList<>();
        List<FileStatus> fileStatuses = listStatus(context);
        for (FileStatus fileStatus : fileStatuses) {
            splits.add(getFileSplit(fileStatus, conf));
        }
        return splits;
    }


    private FileSplit getFileSplit(FileStatus fileStatus, Configuration config) throws IOException {
        FileSystem fs = fileStatus.getPath().getFileSystem(config);
        long start = 0;
        long length = fileStatus.getLen();
        BlockLocation[] blkLocations = fs.getFileBlockLocations(fileStatus, 0, length);

        Multiset<String> hostSet = HashMultiset.create();
        for (BlockLocation location : blkLocations) {
            Collections.addAll(hostSet, location.getHosts());
        }
        Multiset<String> orderHostSet = Multisets.copyHighestCountFirst(hostSet);
        Set<String> uniqueHosts = orderHostSet.elementSet();
        String[] hosts = uniqueHosts.toArray(new String[0]);

        return new FileSplit(fileStatus.getPath(), start, length, hosts);
    }
}
