package com.wegraph.graphson.deduplicate;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.alibaba.fastjson.JSON;
import com.wegraph.graphson.DDConstants;

/**
 * Group edge by from_vertex + to_vertex,
 * Because all edge have direction, we have to keep the from_vertex -> to_vertex order.
 * @author handongqing
 * revised @2018/07/31
 * revised @2018/11/05
 */
public class EdgeMapper extends Mapper<Object, Text, Text, Text> {
    protected final static String NULL      = "\\n";
    /**
     * Mapper INPUT format for edge:
     *     'EDGE' # out_vertex # in_vertex [# edge_property_name : edge_property_value]*
     * Predefined edge property names include EDGE_LABEL, FROM_LABEL, TO_LABEL
     * EDGE_LABEL, FROM_LABEL, TO_LABEL will be placed right after in_vertex successively.
     *     
     * Mapper INPUT format for vertex:
     *     'VERTEX' # out_vertex [# property_name : property_value [: meta_property_name : meta_property_value]* ]*
     * Predefined vertex property names include FROM_LABEL, other properties may have meta properties.
     * FROM_LABEL will be placed right after out_vertex.
     * 
     * '#', ':' are first level and second level separators.
     *
     *
     * Mapper OUTPUT format for edge:
     *     key: out_vertex + in_vertex
     *     value: { property_name : property_value, ...}
     * Predefined property names include: FROM_VERTEX_K, TO_VERTEX_K, EDGE_LABEL_K, FROM_LABEL_K, TO_LABEL_K
     * 
     * Mapper OUTPUT format for vertex:
     *     key: out_vertex
     *     value: { property_name: {PROPERTY_VALUE : property_value [, meta_property_key : meta_property_value]* }, ...}
     * Predefined property names include: FROM_VERTEX_K, FROM_LABEL_K.
     *     If there is multiple property value in an input line, then the mapper emits multiple tuples.
     */
    @Override
    protected void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        
        List<String> tokens = new ArrayList<String>();
        StringTokenizer itr = new StringTokenizer(value.toString(), DDConstants.DELIMITER);
        String rToken       = null;
        while(itr.hasMoreTokens()) {
            rToken = itr.nextToken();
            if (!rToken.equals(NULL)) { tokens.add(rToken); }
        }
        
        if (tokens.get(DDConstants.LINE_TYPE_I).equals(DDConstants.TYPE_EDGE_V)) {
            parse_edge(context, tokens);
        } else {
            parse_vertex(context, tokens);
        }
    }
    
    /** 
     * EMITS edge properties in format: { property_name: property_value, ...}
     * @param context
     * @param key : from_vertex + to_vertex
     * @param tokens
     * @throws IOException
     */
    protected void parse_edge(Context context, final List<String> tokens) throws IOException {
        Map<String, Object> properties_map = new HashMap<String, Object>();
        try {
            // There may be multiple edges between a same pair of vertices with different label.
            String edge_label = null;
            for (int i = DDConstants.TO_VERTEX_I + 1; i < tokens.size(); i++) {
                List<String> properties = new ArrayList<String>(Arrays.asList(tokens.get(i).split(DDConstants.SEPARATOR)));
                if (DDConstants.EDGE_LABEL_K.equals(properties.get(DDConstants.PROPERTY_NAME_I))) {
                    edge_label = properties.get(DDConstants.PROPERTY_VALUE_I);
                    break;
                }
            }
            if (null == edge_label) { throw new IOException("edge has no label error" + tokens.toString()); }
            
            final String key = tokens.get(DDConstants.FROM_VERTEX_I) + edge_label + tokens.get(DDConstants.TO_VERTEX_I);
            properties_map.put(DDConstants.INFO_TYPE_K,     DDConstants.TYPE_EDGE_V);
            properties_map.put(DDConstants.FROM_VERTEX_K,   tokens.get(DDConstants.FROM_VERTEX_I));
            properties_map.put(DDConstants.TO_VERTEX_K,     tokens.get(DDConstants.TO_VERTEX_I));
            context.write(new Text(key), new Text(JSON.toJSONString(properties_map)));
            // There may be identical property keys in one input line,
            // so we have to emit each property key-value pair separately.
            for (int i = DDConstants.TO_VERTEX_I + 1; i < tokens.size(); i++ ) {
                properties_map.clear();
                properties_map.put(DDConstants.INFO_TYPE_K, DDConstants.TYPE_EDGE_V);
                List<String> properties = new ArrayList<String>(Arrays.asList(tokens.get(i).split(DDConstants.SEPARATOR)));

                properties_map.put(properties.get(DDConstants.PROPERTY_NAME_I), properties.get(DDConstants.PROPERTY_VALUE_I));
                context.write(new Text(key), new Text(JSON.toJSONString(properties_map)));
            }
        } catch (Exception es) {
                throw new IOException(tokens.toString());
        }
    }
    /**
     * EMITS vertex properties in format: {property_name : {PROPERTY_VALUE:property_value [, meta_property_key:meta_property_value]*},...}
     * @param context
     * @param key: from_vertex
     * @param tokens
     * @throws IOException
     */
    protected void parse_vertex(Context context, final List<String> tokens) throws IOException {
        Map<String, Map<String, Object>> properties_map = new HashMap<String, Map<String, Object>>();
        try {
            final String key = tokens.get(DDConstants.FROM_VERTEX_I);
            
            properties_map.put(DDConstants.INFO_TYPE_K, new HashMap<String, Object>());
            properties_map.get(DDConstants.INFO_TYPE_K).put(DDConstants.PROPERTY_VALUE, DDConstants.TYPE_VERTEX_V);
            
            properties_map.put(DDConstants.FROM_VERTEX_K, new HashMap<String, Object>());
            properties_map.get(DDConstants.FROM_VERTEX_K).put(DDConstants.PROPERTY_VALUE, tokens.get(DDConstants.FROM_VERTEX_I));
            
            context.write(new Text(key), new Text(JSON.toJSONString(properties_map)));
            
            // There may be identical property keys in one input line,
            // so we have to emit each property key-value pair separately.
            for (int i = DDConstants.FROM_VERTEX_I + 1; i < tokens.size(); i++) {
                properties_map.clear();
                properties_map.put(DDConstants.INFO_TYPE_K, new HashMap<String, Object>());
                properties_map.get(DDConstants.INFO_TYPE_K).put(DDConstants.PROPERTY_VALUE, DDConstants.TYPE_VERTEX_V);
                
                List<String> properties = new ArrayList<String>(Arrays.asList(tokens.get(i).split(DDConstants.SEPARATOR)));

                String property_name = properties.get(DDConstants.PROPERTY_NAME_I);
                properties_map.put(property_name, new HashMap<String, Object>());
                properties_map.get(property_name).put(DDConstants.PROPERTY_VALUE, properties.get(DDConstants.PROPERTY_VALUE_I));
                // meta-property key value pair.
                for (int j = DDConstants.PROPERTY_VALUE_I + 1; j < properties.size(); j += 2) {
                    properties_map.get(property_name).put(properties.get(j), properties.get(j+1));
                }
                context.write(new Text(key), new Text(JSON.toJSONString(properties_map)));
            }
        } catch (Exception es) {
                throw new IOException(tokens.toString());
        }
    }
}
