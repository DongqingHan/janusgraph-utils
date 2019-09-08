package com.wegraph.analysis.oltp;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.util.GraphFactory;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.bothE;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.both;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.has;

import com.wegraph.graphson.DDConstants;


/**
 * @author handongqing01
 * 
 * parallel generating black path feature for input application_ids.
 */
public class UserDistributionMapper extends Mapper<Object, Text, IntWritable, IntWritable> {

    private Graph graph;
    private GraphTraversalSource g;
    
    @Override
    protected void setup(Context context)
            throws IOException, InterruptedException {
        super.setup(context);
        
        if (null == graph) {
            graph = GraphFactory.open(context.getConfiguration().get("graph_configuration_file"));
            try {
                g = graph.traversal();
            } catch (Exception e) {
                try {
                    graph.close();
                } catch (Exception se) {
                    throw new RuntimeException("close graph failed after open traversal failure");
                }
                throw new RuntimeException("open graph traversal failed");
                
            }
        }
      }
    
    @Override
    protected void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        // override this function to do real feature generating task.
        String[] fields = value.toString().split(DDConstants.DELIMITER);
        
        if (fields[DDConstants.EDGE_LABEL_POS].equals(DDConstants.EDGE_LABEL_K + DDConstants.SEPARATOR + "e_register_mobile")) {
            String user_key = fields[DDConstants.FROM_VERTEX_I]; // find the user_key
            Graph subG = (Graph) g.V().has("p_v_name", user_key)
                    .repeat(bothE().and(has("p_e_relation_time_min", P.gte("2018-01-01")),
                            has("p_e_relation_time_min", P.lte("2019-06-25")))
                            .subgraph("subG")
                            .otherV().where(both().count().is(P.lte(30))))
                    .times(3)
                    .cap("subG").next();
            GraphTraversalSource subg = subG.traversal();
            
            if (subg.V().hasLabel("v_user_key").count().next() > 0) {
                context.write(new IntWritable((int) (subg.V().hasLabel("v_user_key").count().next() - 1)), new IntWritable(1));
            } else {
                context.write(new IntWritable(0), new IntWritable(1));
            }
        }
    }
    

    @Override
    protected void cleanup(Context context)
            throws IOException, InterruptedException {
        super.cleanup(context);
        // close graph
        try {
            graph.close();
            graph = null;
        } catch (Exception e) {
            throw new RuntimeException("fail TO close graphs instance");
        }
        
    }

}
