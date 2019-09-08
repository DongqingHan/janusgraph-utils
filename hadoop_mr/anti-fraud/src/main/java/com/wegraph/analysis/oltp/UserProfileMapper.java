package com.wegraph.analysis.oltp;

import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.util.GraphFactory;

import com.alibaba.fastjson.JSON;
import com.wegraph.graphson.DDConstants;


/**
 * @author handongqing01
 * 
 * parallel generating black path feature for input application_ids.
 */
public class UserProfileMapper extends Mapper<Object, Text, Text, Text> {

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
    protected void map(Object key, Text value, Context context) {
        // override this function to do real feature generating task.
        String[] fields = value.toString().split(DDConstants.DELIMITER);
        try {
            if (fields[DDConstants.EDGE_LABEL_POS].equals(DDConstants.EDGE_LABEL_K + DDConstants.SEPARATOR + "e_register_mobile")) {
                String user_key = fields[DDConstants.FROM_VERTEX_I]; // find the user_key
                String mobile = g.V().has("p_v_name", user_key).outE("e_register_mobile").otherV().values("p_v_name").next().toString();
                Number is_passed = g.V().has("p_v_name", user_key).outE("e_application_id").otherV().values("p_v_is_final_passed").sum().next();
                String time = (String) g.V().has("p_v_name", user_key).outE("e_application_id").values("p_e_relation_time_max").toList().stream().max(Comparator.comparing(String::valueOf)).orElse("");
                Map<String, Object> res = new HashMap<>();
                res.put(user_key, is_passed);
                res.put("time", time);
                context.write(new Text(mobile), new Text(JSON.toJSONString(res)));
            }
        } catch (Exception e){
            // NOTHING
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
