package com.wegraph.tools.generator;

import com.wegraph.graphson.DDConstants;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * The Mapper class that given a row number, will generate the appropriate
 * output line.
 */
public class GraphGenMapper
        extends Mapper<LongWritable, NullWritable, Text, NullWritable> {

    static final String edgeLabel = "follow";
    static final String vertexLabel = "person";
    static final String createTime = "p_e_start_time";
    static final String weight = "p_e_weight";
    static final String NUM_VERTICES = "graphgen.num-vertices";
    private Random rnd = null;
    private long verticesNum;

    @Override
    protected void setup(Context context)
            throws IOException, InterruptedException {
        super.setup(context);

        if (null == rnd) {
            rnd = new Random();
        }
        verticesNum = context.getConfiguration().getLong(NUM_VERTICES, 0);
        if (verticesNum == 0) {
            throw new IOException("num-vertices is invalid");
        }
    }
    public void map(LongWritable row, NullWritable ignored,
                    Context context) throws IOException, InterruptedException {
        Integer fromVertex = rnd.nextInt((int) verticesNum);
        Integer toVertex = rnd.nextInt((int) verticesNum);
        while (fromVertex.equals(toVertex)) toVertex = rnd.nextInt((int) verticesNum);
        List<String> result = new ArrayList<>();
        result.add(DDConstants.TYPE_EDGE_V);
        result.add(fromVertex.toString());
        result.add(toVertex.toString());
        result.add(DDConstants.EDGE_LABEL_K + DDConstants.SEPARATOR + edgeLabel);
        result.add(DDConstants.FROM_LABEL_K + DDConstants.SEPARATOR + vertexLabel);
        result.add(DDConstants.TO_LABEL_K + DDConstants.SEPARATOR + vertexLabel);
        result.add(weight + DDConstants.SEPARATOR + "1");
        context.write(new Text(String.join(DDConstants.DELIMITER, result)), NullWritable.get());
    }

    @Override
    public void cleanup(Context context) {
        // NOTHING
    }
}
