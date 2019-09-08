package com.wegraph.analysis.oltp;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;



/**
 * @author handongqing01
 */

public  class UserDistributionWeakReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
    @Override
    protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable ignored : values) {
            sum += 1;
        }
        context.write(key, new IntWritable(sum));
    }
}

