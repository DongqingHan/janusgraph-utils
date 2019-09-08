package com.wegraph.analysis.oltp;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.has;

import java.io.IOException;
import java.util.Set;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.util.GraphFactory;

import com.wegraph.graphson.DDConstants;

public class UserDistWeakMapper extends Mapper<Object, Text, IntWritable, IntWritable> {

    private Graph graph;
    private GraphTraversalSource g;
    
    protected final static String p_v_name = "p_v_name";
    protected final static String e_application_id = "e_application_id";
    protected final static String e_phonebook = "e_phonebook";
    protected final static String e_register_mobile = "e_register_mobile";
    protected final static String e_emergency_call = "e_emergency_call";
    protected final static String e_call = "e_call";
    protected final static String p_e_relation_time_min = "p_e_relation_time_min";
    protected final static String p_e_relation_time_max = "p_e_relation_time_max";
    protected final static String p_e_start_time = "p_e_start_time";
    protected final static String label = "label";
    protected final static String p_v_black_tag = "p_v_black_tag";
    protected final static String p_v_d30_date = "p_v_d30_date";
    protected final static String p_v_is_final_passed = "p_v_is_final_passed";
    protected final static String degree = "degree";
    protected final static String v_user_key = "v_user_key";
    
    
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
            
            Set<Object> s1 = g.V().has(p_v_name, user_key).
            		outE(e_register_mobile, e_emergency_call).
            		and(has("p_e_relation_time_min", P.gte("2019-03-01")),has("p_e_relation_time_min", P.lte("2019-03-31"))).
            		otherV().
            		both(e_call).
            		in(e_register_mobile, e_emergency_call).
            		hasLabel(v_user_key).
            		values(p_v_name).toSet();
            
            
          context.write(new IntWritable(s1.size()), new IntWritable(1));
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
