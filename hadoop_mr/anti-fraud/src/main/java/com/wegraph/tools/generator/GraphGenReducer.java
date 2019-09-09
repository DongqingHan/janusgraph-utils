package com.wegraph.tools.generator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class GraphGenReducer extends Reducer<LongWritable, LongWritable, LongWritable, LongWritable> {
    @Override
    protected void reduce(LongWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        int count = 0;
        for (LongWritable ignored : values) count += 1;
        context.write(key, new LongWritable(count));
    }

}
