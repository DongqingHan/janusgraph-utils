package com.wegraph.graphson.ajacentlist;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.alibaba.fastjson.JSON;
import com.wegraph.graphson.DDConstants;
import com.wegraph.graphson.PAConstants;

/** 
 * The PartMapper input is the output of EdgeReducer.
 * @author handongqing
 * @revised 2018/07/12
 * @revised 2018/11/06
 */
public class PartMapper extends Mapper<Object, Text, Text, Text> {
    /**
     * 1. from vertex, emits   vertex:{INFO_TYPE_K:      TYPE_VERTEX_V,
     *                                 VERTEX_LABEL_P:   vertex_label, 
     *                                 property_name:   [{PROPERTY_VALUE: property_value, 
     *                                                    meta_property_key: meta_property_value
     *                                                    ...
     *                                                    }],
     *                                 ...
     *                                }
     *                                 
     * 2. from edge,emits from_vertex:{INFO_TYPE_K:      TYPE_EDGE_V,
     *                                 EDGE_DIR:         OUT_E,
     *                                 EDGE_ID:          uuid,
     *                                 TO_VERTEX_K:      to_vertex,
     *                                 EDGE_LABEL_P:     edge_label,
     *                                 edge_property_key:edge_property_value,
     *                                 ...
     *                                } and
     *                                 
     *                     to_vertex: {INFO_TYPE_K:      TYPE_EDGE_V,
     *                                 EDGE_DIR:         IN_E,
     *                                 EDGE_ID:          uuid,
     *                                 EDGE_LABEL_P:     edge_label,
     *                                 FROM_VERTEX_K:    from_vertex,
     *                                 edge_property_key:property_value,
     *                                 ...
     *                                } and
     *                                 
     *                    from_vertex:{INFO_TYPE_K:     TYPE_VERTEX_V,
     *                                 VERTEX_LABEL_P:  from_vertex_label
     *                                } and
     *                                 
     *                     to_vertex: {INFO_TYPE_K:      TYPE_VERTEX_V,
     *                                 VERTEX_LABEL_P:  to_vertex_label
     *                                }
     */
    @Override
    protected void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        Map<String, Object> dict = JSON.parseObject(value.toString());
        String info_type = (String) dict.remove(DDConstants.INFO_TYPE_K);
        if (info_type.equals(DDConstants.TYPE_EDGE_V)) {
            // edge information, keep here for later use.
            String edge_label    = (String) dict.remove(DDConstants.EDGE_LABEL_K);
            String out_v         = (String) dict.remove(DDConstants.FROM_VERTEX_K);
            String in_v          = (String) dict.remove(DDConstants.TO_VERTEX_K);
            String from_label    = (String) dict.remove(DDConstants.FROM_LABEL_K);
            String to_label      = (String) dict.remove(DDConstants.TO_LABEL_K);
            
            // emits outE for out_v
            dict.put(PAConstants.INFO_TYPE_K,    PAConstants.TYPE_EDGE_V);
            dict.put(PAConstants.EDGE_ID,        UUID.randomUUID().toString());
            dict.put(PAConstants.EDGE_LABEL_P,   edge_label);
            dict.put(PAConstants.EDGE_DIR,       PAConstants.OUT_E);
            dict.put(PAConstants.TO_VERTEX_K,    in_v);
            context.write(new Text(out_v),       new Text(JSON.toJSONString(dict)));
            
            // emits inE for in_v
            dict.remove(PAConstants.TO_VERTEX_K);
            dict.put(PAConstants.FROM_VERTEX_K, out_v);
            dict.put(PAConstants.EDGE_DIR,    PAConstants.IN_E);
            context.write(new Text(in_v),     new Text(JSON.toJSONString(dict)));
            
            // emits label property for to_v
            Map<String, String> vertex_dict = new HashMap<String, String>();
            vertex_dict.put(PAConstants.INFO_TYPE_K, PAConstants.TYPE_VERTEX_V);
            vertex_dict.put(PAConstants.VERTEX_LABEL_P, from_label);
            context.write(new Text(out_v), new Text(JSON.toJSONString(vertex_dict)));
            
            // emits label property for out_v
            vertex_dict.put(PAConstants.VERTEX_LABEL_P, to_label);
            context.write(new Text(in_v), new Text(JSON.toJSONString(vertex_dict)));
            
        } else if (info_type.equals(DDConstants.TYPE_VERTEX_V)) {
            // vertex info, vertex label and other ordinary properties
            String label = (String) dict.remove(PAConstants.FROM_LABEL_V);
            String out_v = (String) dict.remove(PAConstants.FROM_VERTEX_K);
            dict.put(PAConstants.INFO_TYPE_K,       PAConstants.TYPE_VERTEX_V);
            dict.put(PAConstants.VERTEX_LABEL_P,    label);
            context.write(new Text(out_v), new Text(JSON.toJSONString(dict)));
        } else {
            throw new IOException(value.toString());
        }
    }
}
