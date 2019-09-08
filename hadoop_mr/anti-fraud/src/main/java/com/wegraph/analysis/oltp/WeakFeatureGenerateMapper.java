package com.wegraph.analysis.oltp;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.constant;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.label;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.outE;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.out;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.values;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.has;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.and;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.or;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.select;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.util.GraphFactory;

import com.alibaba.fastjson.JSON;
import com.wegraph.graphson.DDConstants;

public class WeakFeatureGenerateMapper extends Mapper<Object, Text, Text, Text>{
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
    	Map<String, Object> feature_map = new HashMap<>();
    	 String split_str = DDConstants.DELIMITER;
//    	String split_str = ",";
    	String[] fields = value.toString().split(split_str);
    	String user_key = fields[DDConstants.FROM_VERTEX_I]; //+ "_rrd";
    	List<String> min_relation_time_list = new ArrayList<String>();
        try {
        	min_relation_time_list = g.V().has(p_v_name, user_key).
                    coalesce(outE(e_application_id).values(p_e_relation_time_max),
                            constant("")).toList();
            
            String min_relation_time = Collections.max(min_relation_time_list);
            
            if (min_relation_time.isEmpty()) 
        	{
            	context.write(new Text(user_key), new Text("EMPTY"));
            	return;
        	}
            
            List<Object> paths = g.V().has("p_v_name", user_key).outE("e_register_mobile", "e_emergency_call").as("x").otherV().
            		bothE("e_phonebook", "e_call").as("y").
            		choose(label()).
            		    option("e_phonebook", 
            		            has("p_e_start_time", P.lte(min_relation_time)).
            		            otherV().simplePath().where(outE().hasLabel("e_register_mobile")).
            		            project("user_key", "time", "application", "edge1", "edge2").
            		            	by(values("p_v_name")).
            		            	by(outE("e_register_mobile").properties("p_e_relation_time_min").value()).
                                    by(out("e_application_id").valueMap().fold()).
            		            	by(select("x").label()).
                                    by(select("y").label())
            		        ).
            		    option("e_call",
            		           and(has("p_e_start_time", P.lte(min_relation_time)), 
            		        		   or(values("p_e_calls_total_duration").is(P.gte(30)), values("p_e_calls_num").is(P.gte(3))
            		                ))
            		                .otherV().
            		                inE("e_register_mobile", "e_emergency_call").has("p_e_relation_time_min", P.lte(min_relation_time)).
            		                otherV().simplePath().where(outE().hasLabel("e_register_mobile")).
            		                project("user_key", "time", "application", "edge1", "edge2").
            		                	by(values("p_v_name")).
            		                	by(outE("e_register_mobile").properties("p_e_relation_time_min").value()).
                                        by(out("e_application_id").valueMap().fold()).
                		            	by(select("x").label()).
                                        by(select("y").label())
            		    ).toList();
            
            Map<String, Integer> features = new HashMap<String, Integer>();
            Set<String> uk_set = new HashSet<String>();

            List<String> l = new ArrayList<String>();
            l.add("_rrd");
            l.add("_hh");
            l.add("_pdl");
            
            for (Object o: paths){
            	Boolean whether_dup = false;
            	Map<String, Object> userKeyTimeMap = (Map<String, Object>) o;
            	
            	if (uk_set.contains(userKeyTimeMap.get("user_key").toString()))
            	{
            		whether_dup = true;
            	}
            	uk_set.add(userKeyTimeMap.get("user_key").toString());
            	
            	List<Object> applications = (List<Object>) userKeyTimeMap.get("application");
            	Boolean application_is_passed = false;
            	Boolean first_bill_d10 = false;
            	Boolean first_bill_d30 = false;
            	Boolean first_bill_d60 = false;
            	Boolean ever_d10 = false;
            	Boolean ever_d30 = false;
            	Boolean ever_d60 = false;
            	
            	for (Object app: applications)
            	{
            		try {
	            		Map<String, List<Object>> app_valuemap = (Map<String, List<Object>>) app;
	        			if (app_valuemap == null)
	        				continue;
	    				if (app_valuemap.get("p_v_is_final_passed") != null && app_valuemap.get("p_v_is_final_passed").get(0).toString().equals("1"))
	    				{
	    					application_is_passed = true;
	    					if ((!app_valuemap.get("p_v_first_bill_d10").get(0).toString().startsWith("9999")) && (app_valuemap.get("p_v_first_bill_d10").get(0).toString().compareTo("3000-01-01") < 0))
	    					{
	    						first_bill_d10 = true;
	    						ever_d10 = true;
	    					}
	    					else if ((!app_valuemap.get("p_v_d10_date").get(0).toString().startsWith("9999")) && (app_valuemap.get("p_v_d10_date").get(0).toString().compareTo("3000-01-01") < 0))
	    					{
	    						ever_d10 = true;
	    					}
	    					if ((!app_valuemap.get("p_v_first_bill_d30").get(0).toString().startsWith("9999")) && (app_valuemap.get("p_v_first_bill_d30").get(0).toString().compareTo("3000-01-01") < 0))
	    					{
	    						first_bill_d30 = true;
	    						ever_d30 = true;
	    					}
	    					else if ((!app_valuemap.get("p_v_d30_date").get(0).toString().startsWith("9999")) && (app_valuemap.get("p_v_d30_date").get(0).toString().compareTo("3000-01-01") < 0))
	    					{
	    						ever_d30 = true;
	    					}
	    					if ((!app_valuemap.get("p_v_first_bill_d60").get(0).toString().startsWith("9999")) && (app_valuemap.get("p_v_first_bill_d60").get(0).toString().compareTo("3000-01-01") < 0))
	    					{
	    						first_bill_d60 = true;
	    						ever_d60 = true;
	    					}
	    					else if ((!app_valuemap.get("p_v_d60_date").get(0).toString().startsWith("9999")) && (app_valuemap.get("p_v_d60_date").get(0).toString().compareTo("3000-01-01") < 0))
	    					{
	    						ever_d60 = true;
	    					}
	    				}
	    				
            		}
            		catch (Exception e){
    					continue;
    				}
            		
            	}
                

            	for (String system_id: l)
        		{
            		if (userKeyTimeMap.get("user_key").toString().endsWith(system_id))
            		{
		            	if (userKeyTimeMap.get("time").toString().compareTo("2018-01-01 00:00:00") < 0)
		            	{	
	            			add_feature_map(features, whether_dup, system_id, "user_key_before18");
	            			if (applications.size() > 0)
	            			{
		            			add_feature_map(features, whether_dup, system_id, "user_key_before18_with_application");
		            			if (application_is_passed)
		            			{
		            				application_features(features, whether_dup, system_id, "before18", 
		            		    		       first_bill_d10, first_bill_d30, first_bill_d60, 
		            		    		       ever_d10, ever_d30, ever_d60);
		            			}
	            			}
	            			add_feature_map(features, whether_dup, system_id, "user_key_before18_by_" + userKeyTimeMap.get("edge1") + "_" + userKeyTimeMap.get("edge2"));
		            	}
		            	else{
	            			add_feature_map(features, whether_dup, system_id, "user_key_after18");
	            			if (applications.size() > 0)
	            			{
		            			add_feature_map(features, whether_dup, system_id, "user_key_after18_with_application");
		            			if (application_is_passed)
		            			{
		            				application_features(features, whether_dup, system_id, "after18", 
		            		    		       first_bill_d10, first_bill_d30, first_bill_d60, 
		            		    		       ever_d10, ever_d30, ever_d60);
		            			}
	            			}
	            			add_feature_map(features, whether_dup, system_id, "user_key_after18_by_" + userKeyTimeMap.get("edge1") + "_" + userKeyTimeMap.get("edge2"));

		            	}
            			add_feature_map(features, whether_dup, system_id, "user_key_all");
            			add_feature_map(features, whether_dup, system_id, "user_key_all_by_" + userKeyTimeMap.get("edge1") + "_" + userKeyTimeMap.get("edge2"));
            			if (applications.size() > 0)
            			{
	            			add_feature_map(features, whether_dup, system_id, "user_key_all_with_application");
	            			if (application_is_passed)
	            			{
	            				application_features(features, whether_dup, system_id, "all", 
	            		    		       first_bill_d10, first_bill_d30, first_bill_d60, 
	            		    		       ever_d10, ever_d30, ever_d60);
	            			}
            			}
            		}
        		}

            }
            
            context.write(new Text(user_key), new Text(JSON.toJSONString(features)));

        } catch (Exception e) {
        	context.write(new Text(user_key), new Text(e.toString()));
            return;
        }
    }
    
    protected void add_feature_map(Map<String, Integer> features, Boolean whether_dup, String system_id, String feature_name)
    {
    	features.put(feature_name + system_id, features.getOrDefault(feature_name + system_id, 0) + 1);
		if (whether_dup == false)
			features.put("dedup_" + feature_name + system_id, features.getOrDefault("dedup_" + feature_name + system_id, 0) + 1);
    }
    
    protected void application_features(Map<String, Integer> features, Boolean whether_dup, String system_id, String feature_name, 
    		       Boolean first_bill_d10, Boolean first_bill_d30, Boolean first_bill_d60, 
    		       Boolean ever_d10, Boolean ever_d30, Boolean ever_d60)
    {
    	add_feature_map(features, whether_dup, system_id, "user_key_" + feature_name + "_with_application_passed");
		if (first_bill_d10)
		{
			add_feature_map(features, whether_dup, system_id, "user_key_" + feature_name + "_with_application_firstbill_d10");
		}
		if (first_bill_d30)
		{
			add_feature_map(features, whether_dup, system_id, "user_key_" + feature_name + "_with_application_firstbill_d30");
		}
		if (first_bill_d60)
		{
			add_feature_map(features, whether_dup, system_id, "user_key_" + feature_name + "_with_application_firstbill_d60");
		}
		if (ever_d10)
		{
			add_feature_map(features, whether_dup, system_id, "user_key_" + feature_name + "_with_application_ever_d10");
		}
		if (ever_d30)
		{
			add_feature_map(features, whether_dup, system_id, "user_key_" + feature_name + "_with_application_ever_d30");
		}
		if (ever_d60)
		{
			add_feature_map(features, whether_dup, system_id, "user_key_" + feature_name + "_with_application_ever_d60");
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
