package com.ntu.bdm.util;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.io.LongWritable;
import java.io.IOException;

/**
 * Class does nothing except returning the customized record reader.
 */

public class LineNumInputFormat extends FileInputFormat {


    @Override
    public RecordReader<LongWritable, Text> createRecordReader(InputSplit split,
                                                               TaskAttemptContext context) throws IOException {
        LineNumRecordReader reader = new LineNumRecordReader();
        return reader;
    }

}