package com.wegraph.analysis.collector;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import com.wegraph.graphson.DDConstants;

/**
 * @author handongqing01
 *
 */
public class DegreeMapper extends Mapper<Object, Text, Text, IntWritable> {
    private final static IntWritable outCount = new IntWritable(1);
    private final static IntWritable inCount = new IntWritable(-1);
    
    @Override
    protected void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        StringTokenizer itr = new StringTokenizer(value.toString(), DDConstants.DELIMITER);
        String type_str     = itr.nextToken();
        String from_vt      = itr.nextToken();
        String to_vt        = itr.nextToken();
        
        if (DDConstants.TYPE_EDGE_V.equals(type_str)) {
            context.write(new Text(from_vt), outCount);
            context.write(new Text(to_vt), inCount);
        }
    }
}
