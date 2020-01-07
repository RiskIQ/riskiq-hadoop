package com.riskiq.mapreduce.io;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author ahunt
 */
public class FilenameRecordReader extends RecordReader<IntWritable, Text> {
    private static final Pattern partitionPattern = Pattern.compile("-(\\d{5})\\.?");


    private boolean hasRun = false;

    private Text filename;
    private IntWritable partition;


    @Override
    public void initialize(InputSplit genericSplit, TaskAttemptContext context) throws IOException, InterruptedException {
        FileSplit split = (FileSplit) genericSplit;
        Path path = split.getPath();
        String file = path.getName();

        int id = getPartitionID(file);

        filename = new Text(path.toString());
        partition = new IntWritable(id);
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (!hasRun) {
            hasRun = true;
            return true;
        }

        return false;
    }

    @Override
    public IntWritable getCurrentKey() throws IOException, InterruptedException {
        return partition;
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
        return filename;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return hasRun ? 1.0f : 0.0f;
    }

    @Override
    public void close() throws IOException {

    }


    private int getPartitionID(String filename) {
        int id = -1;

        Matcher matcher = partitionPattern.matcher(filename);
        while (matcher.find()) {
            id = Integer.parseInt(matcher.group(1));
        }

        return id;
    }
}
