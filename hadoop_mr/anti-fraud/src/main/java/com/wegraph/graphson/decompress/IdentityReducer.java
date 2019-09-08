package com.wegraph.graphson.decompress;

import java.io.IOException;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


/**
 * @author handongqing01
 */

public  class IdentityReducer extends Reducer<Text, NullWritable, NullWritable, Text> {
    @Override
    protected void reduce(Text key, Iterable<NullWritable> values, Context context)
            throws IOException, InterruptedException {
        context.write(NullWritable.get(), key);
    }
}

