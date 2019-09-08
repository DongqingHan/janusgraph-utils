package com.wegraph.tools.decompress;


import java.io.IOException;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author handongqing01
 *
 */
public class IdentityMapper extends Mapper<Object, Text, Text, NullWritable> {
    @Override
    protected void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
            context.write(value, NullWritable.get());
    }
}
