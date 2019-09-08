package com.wegraph.analysis.collector;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.wegraph.graphson.DDConstants;


/**
 * @author handongqing01
 */

public  class DegreeReducer extends Reducer<Text, IntWritable, NullWritable, Text> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        int threshold = conf.getInt("THRESHOLD", 0);
        int outSum = 0;
        int inSum = 0;
        for (IntWritable val : values) {
            int value = val.get();
            if (value > 0 ) {
                outSum += value;
            } else {
                inSum -= value;
            }
        }
        if ((outSum + inSum) >= threshold) {
            context.write(NullWritable.get(), new Text(String.join(DDConstants.DELIMITER, key.toString(), Integer.toString(outSum + inSum), Integer.toString(outSum), Integer.toString(inSum))));
        }
    }
}

