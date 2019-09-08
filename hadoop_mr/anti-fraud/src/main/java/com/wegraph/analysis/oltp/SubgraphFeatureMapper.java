package com.wegraph.analysis.oltp;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Collections;
import com.alibaba.fastjson.JSON;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.util.GraphFactory;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.bothE;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.outE;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.label;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.values;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.project;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.coalesce;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.constant;

import com.wegraph.graphson.DDConstants;


/**
 * @author handongqing01
 * 
 * parallel generating feature for input user_key.
 */
public class SubgraphFeatureMapper extends Mapper<Object, Text, Text, Text> {

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
    
    protected final static String min_time = "2019-01-01 00:00:00";
    protected final static String max_time = "2019-01-31 24:59:59";
    
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
        Map<String, Object> feature_map = new HashMap<String, Object>();
        String[] fields = value.toString().split(DDConstants.DELIMITER);
        String user_key = fields[DDConstants.FROM_VERTEX_I];
        // phonebook_1_hop feature
        //  application <-- user_key -..-> mobile <-- user_key --> application
        //  application <-- user_key --> mobile <-..- user_key <-- application
        String min_relation_time = "";
        try {
            min_relation_time = g.V().has(p_v_name, user_key).
                    coalesce(outE(e_application_id).values(p_e_relation_time_max),
                            constant("")).next();
            if (min_relation_time.isEmpty() ||
                    min_time.compareTo(min_relation_time) > 0 ||
                    max_time.compareTo(min_relation_time) < 0) return;
            
            List<String> applications = g.V().has(p_v_name, user_key).
                    out(e_application_id).has(p_v_is_final_passed, 1).
                    values(p_v_d30_date).map((p) -> "" + p).toList();
            if (applications.isEmpty()) return;
            String min_overdue = Collections.min(applications, String.CASE_INSENSITIVE_ORDER);
            feature_map.put(p_v_d30_date, min_overdue);
        } catch (Exception e) {
            return;
        }
        
        List<Path> paths = g.V().has(p_v_name, user_key).
                outE(e_phonebook).
                inV().
                inE(e_register_mobile, e_emergency_call).has(p_e_relation_time_min, P.lt(min_relation_time)).
                outV().
                until(outE("e_application_id").count().is(0)).repeat(outE(e_application_id).inV()).
                simplePath().path().
                by(project(p_v_black_tag, p_v_d30_date).
                        by(coalesce(values(p_v_black_tag),constant(""))).
                        by(coalesce(values(p_v_d30_date), constant("")))).
                by(label()).toList();
        
        List<Path> paths_1 = g.V().has(p_v_name, user_key).
                outE(e_register_mobile, e_emergency_call).
                inV().
                inE(e_phonebook).has(p_e_start_time, P.lt(min_relation_time)).
                outV().
                until(outE(e_application_id).count().is(0)).repeat(outE(e_application_id).inV()).
                simplePath().path().
                by(project(p_v_black_tag, p_v_d30_date).
                        by(coalesce(values("p_v_black_tag"),constant(""))).
                        by(coalesce(values("p_v_d30_date"), constant("")))).
                by(label()).toList();

        for(Path path: paths_1) {
            paths.add(path);
        }
        
        double d30_num_value = 0.0;
        double blacktag_num_value = 0.0;
        for(Path path: paths) {
            List<Object> elements = path.objects();
            Map<String, Object> step = (Map<String, Object>) elements.get(elements.size() - 1);
            if (step.containsKey(p_v_d30_date) && !((String)step.get(p_v_d30_date)).isEmpty()) {
                d30_num_value += "2019-12-31 00:00:00".compareTo((String) step.get(p_v_d30_date)) < 0 ? 0.0 : 1.0;
            }
            if (step.containsKey(p_v_black_tag) && !((String) step.get(p_v_black_tag)).isEmpty()) {
                blacktag_num_value += 1.0;
            }
        }
        
        Map<String, Double> phonebook_feature_1_hop = new HashMap<>();
        phonebook_feature_1_hop.put(p_v_d30_date, d30_num_value);
        phonebook_feature_1_hop.put(p_v_black_tag, blacktag_num_value);
        feature_map.put("phonebook_feature_1_hop", phonebook_feature_1_hop);
        
        // phonebook_2_hop feature
        //  application <-- user_key -..-> mobile/degree <-..- user_key --> application
        try {
            min_relation_time = g.V().has(p_v_name, user_key).
                    coalesce(outE(e_phonebook).values(p_e_start_time),
                            constant("")).next();
            if(min_relation_time.isEmpty()) {
                ;
            } else {
                paths = g.V().has(p_v_name, user_key).
                        outE(e_phonebook).
                        inV().where(bothE().count().is(P.lt(100))).
                        inE(e_phonebook).has(p_e_start_time, P.lt(min_relation_time)).
                        outV().
                        until(outE(e_application_id).count().is(0)).repeat(outE(e_application_id).inV()).
                        simplePath().path().
                        by(project(degree, p_v_black_tag, p_v_d30_date).
                                by(bothE().count()).
                                by(coalesce(values(p_v_black_tag),constant(""))).
                                by(coalesce(values(p_v_d30_date), constant("")))).
                        by(label()).toList();
                d30_num_value = 0.0;
                blacktag_num_value = 0.0;
                
                for(Path path: paths) {
                    List<Object> elements = path.objects();
                    Map<String, Object> step = (Map<String, Object>) elements.get(elements.size() - 1);
                    Map<String, Object> middle_step = (Map<String, Object>) elements.get(2);
                    if (step.containsKey(p_v_d30_date) && !((String)step.get(p_v_d30_date)).isEmpty()) {
                        d30_num_value += "2019-12-31 00:00:00".compareTo((String) step.get(p_v_d30_date)) < 0 ?
                                0.0 : 1.0 / (Double) middle_step.get(degree);
                    }
                    if (step.containsKey(p_v_black_tag) && !((String) step.get(p_v_black_tag)).isEmpty()) {
                        blacktag_num_value += 1.0 / (Double) middle_step.get(degree);
                    }
                }
                Map<String, Double> phonebook_feature_2_hop = new HashMap<>();
                phonebook_feature_2_hop.put(p_v_d30_date, d30_num_value);
                phonebook_feature_2_hop.put(p_v_black_tag, blacktag_num_value);
                feature_map.put("phonebook_feature_2_hop", phonebook_feature_2_hop);
            }
        } catch (Exception e) {
            ;
        }
        
        // callrec_1_hop feature
        //  application <-- user_key --> mobile -..-> mobile <-- user_key --> application 
        //  application <-- user_key --> mobile <-..- mobile <-- user_key --> application
        try {
            min_relation_time = (String) g.V().has(p_v_name, user_key).
                    coalesce(outE(e_register_mobile).inV().bothE(e_call).values(p_e_start_time),
                            outE(e_application_id).values(p_e_relation_time_min)).next();
            if(min_relation_time.isEmpty()) {
                ;
            } else {
                paths = g.V().has(p_v_name, user_key).
                        outE(e_register_mobile, e_emergency_call).
                        inV().
                        bothE(e_call).has(p_e_start_time, P.lte(min_relation_time)).
                        otherV().
                        inE(e_register_mobile, e_emergency_call).has(p_e_relation_time_min, P.lt(min_relation_time)).
                        outV().
                        until(outE(e_application_id).count().is(0)).repeat(outE(e_application_id).inV()).
                        simplePath().path().
                        by(project(p_v_black_tag, p_v_d30_date).
                                by(coalesce(values(p_v_black_tag), constant(""))).
                                by(coalesce(values(p_v_d30_date), constant("")))).
                        by(label()).toList();
                
                d30_num_value = 0.0;
                blacktag_num_value = 0.0;
                for(Path path: paths) {
                    List<Object> elements = path.objects();
                    Map<String, Object> step = (Map<String, Object>) elements.get(elements.size() - 1);
                    if (step.containsKey(p_v_d30_date) && !((String)step.get(p_v_d30_date)).isEmpty()) {
                        d30_num_value += "2019-12-31 00:00:00".compareTo((String) step.get(p_v_d30_date)) < 0 ?
                                0.0 : 1.0;
                    }
                    if (step.containsKey(p_v_black_tag) && !((String) step.get(p_v_black_tag)).isEmpty()) {
                        blacktag_num_value += 1.0;
                    }
                }
                Map<String, Double> callrec_feature_1_hop = new HashMap<>();
                callrec_feature_1_hop.put(p_v_d30_date, d30_num_value);
                callrec_feature_1_hop.put(p_v_black_tag, blacktag_num_value);
                feature_map.put("callrec_feature_1_hop", callrec_feature_1_hop);
            }

        } catch (Exception e) {
            ;
        }
        // callrec_2_hop feature
        //  application <-- user_key --> mobile -..-> mobile/degree -..-> mobile <-- user_key --> application
        //  application <-- user_key --> mobile -..-> mobile/degree <-..- mobile <-- user_key --> application
        //  application <-- user_key --> mobile <-..- mobile/degree -..-> mobile <-- user_key --> application
        //  application <-- user_key --> mobile <-..- mobile/degree <-..- mobile <-- user_key --. application
        try {
            min_relation_time = (String) g.V().has(p_v_name, user_key).
                    coalesce(outE(e_register_mobile).inV().bothE(e_call).values(p_e_start_time),
                            outE(e_application_id).values(p_e_relation_time_min)).next();
            if(min_relation_time.isEmpty()) {
                ;
            } else {
                paths = g.V().has(p_v_name, user_key).
                        outE(e_register_mobile, e_emergency_call).
                        inV().
                        bothE(e_call).has(p_e_start_time, P.lte(min_relation_time)).
                        otherV().where(bothE().count().is(P.lt(100))).
                        bothE(e_call).has(p_e_start_time, P.lt(min_relation_time)).
                        otherV().
                        inE(e_register_mobile, e_emergency_call).has(p_e_relation_time_min, P.lt(min_relation_time)).
                        outV().
                        until(outE(e_application_id).count().is(0)).repeat(outE(e_application_id).inV()).
                        simplePath().path().
                        by(project(degree, p_v_black_tag, p_v_d30_date).
                                by(bothE().count()).
                                by(coalesce(values(p_v_black_tag),constant(""))).
                                by(coalesce(values(p_v_d30_date), constant("")))).
                        by(label()).toList();
                
                d30_num_value = 0.0;
                blacktag_num_value = 0.0;
                for(Path path: paths) {
                    List<Object> elements = path.objects();
                    Map<String, Object> step = (Map<String, Object>) elements.get(elements.size() - 1);
                    Map<String, Object> middle_step = (Map<String, Object>) elements.get(2);
                    if (step.containsKey(p_v_d30_date) && !((String)step.get(p_v_d30_date)).isEmpty()) {
                        d30_num_value += "2019-12-31 00:00:00".compareTo((String) step.get(p_v_d30_date)) < 0 ?
                                0.0 : 1.0 / (Double) middle_step.get(degree);
                    }
                    if (step.containsKey(p_v_black_tag) && !((String) step.get(p_v_black_tag)).isEmpty()) {
                        blacktag_num_value += 1.0 / (Double) middle_step.get(degree);
                    }
                }
                Map<String, Double> callrec_feature_2_hop = new HashMap<String, Double>();
                callrec_feature_2_hop.put(p_v_d30_date, d30_num_value);
                callrec_feature_2_hop.put(p_v_black_tag, blacktag_num_value);
                feature_map.put("callrec_feature_2_hop", callrec_feature_2_hop);
            }
        } catch (Exception e) {
            ;
        }
        
        context.write(new Text(user_key), new Text(JSON.toJSONString(feature_map)));
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
