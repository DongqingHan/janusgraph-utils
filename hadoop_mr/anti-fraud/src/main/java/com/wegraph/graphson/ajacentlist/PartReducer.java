package com.wegraph.graphson.ajacentlist;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.Map.Entry;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.alibaba.fastjson.JSON;
import com.wegraph.graphson.DDConstants;
import com.wegraph.graphson.PAConstants;

/**
 * @author handongqing
 * @revised 2018/07/12
 * @revised 2018/11/06
 */
public class PartReducer extends Reducer<Text, Text, NullWritable, Text> {

    // top-most level of GraphSon structure
    private Map<String, Object> adjacent_list   = null;
    // label and properties for in edge
    private Map<String, Object> inE             = null;
    // label and properties for out edge
    private Map<String, Object> outE            = null;
    // vertex properties
    private Map<String, Object> properties      = null;
    private String v_key                        = null;

    /**
     * GraphSon format:
     *         {
     *         'id': xxxx,    // vertex label + vertex value
     *         'label': xxxx,
     *         'outE': outE,    // edge id uses uuid which should be unique with vertex ids.
     *         'inE' : inE,
     *         'properties': {
     *                     property_name: [
     *                             {"id": property_id,    // vertex value + property_name + index or using uuid
     *                              "value": property_value,
     *                              "properties": {
     *                                     meta_property_name: meta_property_value,
     *                                     ...
     *                                  }
     *                             },
     *                             ...
     *                     ],
     *                     ...
     *         }
     *     }
     */
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        // instance variable reset.
        inE = new HashMap<String, Object>();
        outE = new HashMap<String, Object>();
        properties = new HashMap<String, Object>();
        adjacent_list = new HashMap<String, Object>();
        v_key = key.toString();
            
        Map<String, String> name_property = new HashMap<>();
        name_property.put(DDConstants.PROPERTY_VALUE, v_key);
        List<Map<String, String>> name_property_list = new ArrayList<>();
        name_property_list.add(name_property);
        properties.put("p_v_name", name_property_list);
        
        for (Text val:values) {
            Map<String, Object> dict = JSON.parseObject(val.toString());
            switch ((String)dict.get(PAConstants.INFO_TYPE_K)) {
            case PAConstants.TYPE_VERTEX_V:
                vertex_info_process(dict);
                break;
            case PAConstants.TYPE_EDGE_V:
                if (((String)dict.get(PAConstants.EDGE_DIR)).equals(PAConstants.OUT_E)) {
                    merge_outE(dict);
                } else {
                    merge_inE(dict);
                }
                break;
            default:
                throw new IOException(val.toString());
            }
        }
        // Format properties
        format_property();
        adjacent_list.put("properties", properties); //GraphSon format, graphSon keyword
        format_edge();
        
        if (!inE.isEmpty()) {
            adjacent_list.put(PAConstants.IN_E, inE); //GraphSon keyword
        }
        if (!outE.isEmpty()) {
            adjacent_list.put(PAConstants.OUT_E, outE); //GraphSon keyword
        }
        context.write(NullWritable.get(), new Text(JSON.toJSONString(adjacent_list)));
    }
    
    /**
     * properties original format:
     * { "id":xxx, "label":xxx, "property_name": [{PROPERTY_VALUE: value, }], ...}
     */
    private void vertex_info_process(Map<String, Object> dict) {
        for (Entry<String, Object> entry: dict.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();
            switch (key) {
            case PAConstants.INFO_TYPE_K:
                break;
            case PAConstants.VERTEX_LABEL_P:
                // set vertex label and id in GraphSon format, vertex id consists of label and vertex value
                adjacent_list.put(PAConstants.VERTEX_LABEL_P, (String) value);
                adjacent_list.put(PAConstants.VERTEX_ID, v_key);
                break;
            default:
                assert value instanceof List;
                properties.put(key, value);
            }
        }
    }
    
    /**
     *  input dict format:
     *     {INFO_TYPE:"edge_info", "EDGE_DIR":"outE", "inV":xxxx, "label":xxxx, "id":xxxx, ...}
     * OutE in format:
     * "outE": { 
     *          edge_label_1: [
     *                         { 
     *                          "id":edge_id, 
     *                          "inV": to_vertex, 
     *                          "properties": { 
     *                                         property_name: property_value, 
     *                                         ...
     *                                        }
     *                         }, 
     *                         ... 
     *                        ],
     *          ...
     *         }
     */
    private void merge_outE(Map<String, Object> dict) {
        String edge_label = (String)dict.get(PAConstants.EDGE_LABEL_P);
        if (null == outE.get(edge_label)) {
            outE.put(edge_label, new ArrayList<Map<String, Object>>());
        }
        List<Map<String, Object>> edge_list = (List<Map<String, Object>>) outE.get(edge_label);
        Map<String, Object> edge            = new HashMap<String, Object>();
        Map<String, Object> property        = new HashMap<String, Object>();
        for (Entry<String, Object> entry: dict.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();

            switch (key) {
            case PAConstants.INFO_TYPE_K:
            case PAConstants.EDGE_DIR:
            case PAConstants.EDGE_LABEL_P:
                break;
            case PAConstants.EDGE_ID:
            case PAConstants.TO_VERTEX_K:
                edge.put(key, value);
                break;
            default:
                property.put(key, value);
            }
        }
        if ( !properties.isEmpty() ) { edge.put("properties", property); }
        edge_list.add(edge);
    }
    private void merge_inE(Map<String, Object> dict) {
        String edge_label = (String)dict.get(PAConstants.EDGE_LABEL_P);
        if (null == inE.get(edge_label)) {
            inE.put(edge_label, new ArrayList<Map<String, Object>>());
        }
        List<Map<String, Object>> edge_list = (List<Map<String, Object>>) inE.get(edge_label);
        Map<String, Object> edge            = new HashMap<String, Object>();
        Map<String, Object> property        = new HashMap<String, Object>();
        for (Entry<String, Object> entry: dict.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();
            
            switch (key) {
            case PAConstants.INFO_TYPE_K:
            case PAConstants.EDGE_DIR:
            case PAConstants.EDGE_LABEL_P:
                break;
            case PAConstants.EDGE_ID:
            case PAConstants.FROM_VERTEX_K:
                edge.put(key, value);
                break;
            default:
                property.put(key, value);
            }
        }
        if ( !properties.isEmpty() ) { edge.put("properties", property); }
        edge_list.add(edge);
    }
    
    /**
     *   add id to vertex properties: v_property + v_key, vertex properties format:
     *   properties: {
     *                 property_name:[
     *                                { id:xxx,
     *                                  value:yyy,
     *                                  properties:{
     *                                              meta_property_name:meta_property_value,
     *                                              ...
     *                                             },
     *                                },
     *                                ...
     *                               ],
     *                  ...
     *                 }
     * @throws IOException 
     */
    private void format_property() throws IOException {
        for (Map.Entry<String, Object> entry: properties.entrySet()) {
            try {
                List<Map<String, Object>> property_list = (List<Map<String, Object>>) entry.getValue();
                
                for (Map<String, Object> property_value: property_list ) {
                    // [TODO] It is necessary to make property id unique. Adding property_value and v_key is ugly.
                    property_value.put(PAConstants.PROPERTY_ID, UUID.randomUUID().toString());
                    property_value.put(PAConstants.VALUE, property_value.get(DDConstants.PROPERTY_VALUE));
                    
                    // collect and format meta-properties.
                    Map<String, Object> meta_properties = new HashMap<String, Object>();
                    List<String> meta_property_keys     = new ArrayList<>();
                    meta_property_keys.add(DDConstants.PROPERTY_VALUE);
                    for (Map.Entry<String, Object> property_pair: property_value.entrySet()) {
                        String key = property_pair.getKey();
                        if ( !PAConstants.PROPERTY_ID.equals(key) && !PAConstants.VALUE.equals(key) && !DDConstants.PROPERTY_VALUE.equals(key)) {
                            meta_properties.put(key, property_pair.getValue());
                            meta_property_keys.add(key);
                        }
                    }
                    if ( !meta_properties.isEmpty() ) {
                        property_value.put(PAConstants.PROPERTIES, meta_properties);
                    }
                    // remove key not in GraphSon format and keys already moved to meta_properties.
                    for (String key: meta_property_keys) {
                        property_value.remove(key);
                    }
                }
            } catch (ClassCastException e) {
                throw new IOException("Data format error:" + properties.toString());
            }
        }
    }
    private void format_edge() throws IOException {
        ;
    }
}
