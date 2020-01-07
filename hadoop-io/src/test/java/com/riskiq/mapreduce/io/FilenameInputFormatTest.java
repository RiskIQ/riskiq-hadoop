package com.riskiq.mapreduce.io;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Joe Linn
 * 01/07/2020
 */
public class FilenameInputFormatTest {
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();


    @Test
    public void testGetSplits() throws Exception {
        File inputDir = temporaryFolder.newFolder("input");
        final int numFiles = 10;
        List<Path> inputFiles = new ArrayList<>(numFiles);
        for (int i = 0; i < numFiles; i++) {
            File inputFile = new File(inputDir, i + ".txt");
            FileUtils.write(inputFile, String.valueOf(i));
            inputFiles.add(new Path("file://" + inputFile.getAbsolutePath()));
        }


        Job job = mock(Job.class);
        Configuration conf = new Configuration();
        when(job.getConfiguration()).thenReturn(conf);

        FileInputFormat.addInputPath(job, new Path(inputDir.getAbsolutePath()));

        FilenameInputFormat inputFormat = new FilenameInputFormat();
        List<InputSplit> splits = inputFormat.getSplits(job);

        assertThat(splits, hasSize(numFiles));
        for (InputSplit split : splits) {
            assertThat(inputFiles, hasItem(((FileSplit) split).getPath()));
        }
    }
}