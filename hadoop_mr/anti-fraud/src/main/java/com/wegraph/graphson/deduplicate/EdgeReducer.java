package com.wegraph.graphson.deduplicate;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.alibaba.fastjson.JSON;
import com.wegraph.graphson.DDConstants;

/**
 * @author handongqing
 * revised @2018/07/31
 * revised @2019/06/16
 */
public  class EdgeReducer extends Reducer<Text, Text, NullWritable, Text> {
    /**
     *Reducer is responsible for edge and vertex property merging.
     * Reducer output format for edge:
     *      { INFO_TYPE_K:     edge_info,
     *        EDGE_LABEL_K:      edge label,
     *        FROM_VERTEX_K:   from_vertex id,
     *        TO_VERTEX_K:     to_vertex id,
     *        FROM_LABEL_K:    from_vertex label, 
     *        TO_LABEL_K:      to_vertex label,
     *        property_name:   property value,
     *        ....
     *        }
     * Reducer output format for vertex:
     *      { INFO_TYPE_K:     vertex_info,
     *        FROM_VERTEX_K:   from_vertex id,
     *        FROM_LABEL_K:    from_vertex label,
     *        property_name:   [{PROPERTY_VALUE: value, meta_property_key: meta_property_value, ...},...]
     *      }
     */
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        Map<String, Object> result_map = new HashMap<>();
        Map<String, Object> last_dict = null;
        for (Text vl: values) {
            Map<String, Object> dict = JSON.parseObject(vl.toString());
            
            if (last_dict != null
                    && (dict.get(DDConstants.INFO_TYPE_K) instanceof String && last_dict.get(DDConstants.INFO_TYPE_K) instanceof Map
                            || dict.get(DDConstants.INFO_TYPE_K) instanceof Map && last_dict.get(DDConstants.INFO_TYPE_K) instanceof String)) {
                throw new IOException(dict.toString() + last_dict.toString());
            } else {
                last_dict = dict;
            }
            
            if (dict.get(DDConstants.INFO_TYPE_K) instanceof String) {
                for (Map.Entry<String, Object> entry : dict.entrySet()) {
                    String property_name = entry.getKey();
                    Object property_value = entry.getValue();
                    switch (property_name) {
                    case DDConstants.INFO_TYPE_K:
                    case DDConstants.TO_VERTEX_K:
                    case DDConstants.FROM_VERTEX_K:
                    case DDConstants.TO_LABEL_K:
                    case DDConstants.FROM_LABEL_K:
                    case DDConstants.EDGE_LABEL_K:
                        result_map.put(property_name, identity_merge((String) result_map.get(property_name), (String) property_value));
                        break;
                    case "p_e_start_time":
                    case "p_e_relation_time_min":
                        result_map.put(property_name, minimum_time((String) result_map.get(property_name), (String) property_value));
                        break;
                    case "p_e_phonebook_name":
                    case "p_e_relation_time_max":
                        result_map.put(property_name, maximum_time((String) result_map.get(property_name), (String) property_value));
                        break;
                    case "p_e_calls_num":
                    case "p_e_calls_total_duration":
                        result_map.put(property_name, maximum_integer((Integer) result_map.get(property_name), Integer.parseInt((String) property_value)));
                        break;
                    case "p_e_weight":
                        result_map.put(property_name, sum_weight((Integer) result_map.get(property_name), Integer.parseInt((String) property_value)));
                        break;
                    default:
                        throw new IOException(vl.toString());
                    }
                }
            } else if (dict.get(DDConstants.INFO_TYPE_K) instanceof Map) {
                for (Map.Entry<String, Object> entry : dict.entrySet()) {
                    String property_name = entry.getKey();
                    Map<String, Object> property_value = (Map<String, Object>) entry.getValue();
                    switch (property_name) {
                    case DDConstants.INFO_TYPE_K:
                    case DDConstants.FROM_VERTEX_K:
                    case DDConstants.FROM_LABEL_K:
                        result_map.put(property_name, identity_merge((String) result_map.get(property_name), (String) property_value.get(DDConstants.PROPERTY_VALUE)));
                        break;
                    case DDConstants.TO_LABEL_K:
                        result_map.put(DDConstants.FROM_LABEL_K, identity_merge((String) result_map.get(DDConstants.FROM_LABEL_K), (String) property_value.get(DDConstants.PROPERTY_VALUE)));
                        break;
                    case "p_v_d10_date":
                    case "p_v_d30_date":
                    case "p_v_d60_date":
                    case "p_v_first_bill_d10":
                    case "p_v_first_bill_d30":
                    case "p_v_first_bill_d60":
                        result_map.put(property_name, minimum_date((List<Map<String, String>>) result_map.get(property_name), property_value));
                        break;
                    case "p_v_black_tag":
                    case "p_v_bairong_income":
                    case "p_v_tongdun_income":
                        result_map.put(property_name, list_append((List<Map<String, Object>>) result_map.get(property_name), property_value));
                        break;
                    case "p_v_is_final_passed":
                    case "p_v_callrec_added":
                    case "p_v_phonebook_added":
                    case DDConstants.UDEF_IS_BIG_VERTEX:
                        result_map.put(property_name, final_passed_merge((List<Map<String, Integer>>) result_map.get(property_name), property_value));
                        break;
                    case DDConstants.UDEF_INE_BIG_VERTEX:
                    case DDConstants.UDEF_OUTE_BIG_VERTEX:
                    case DDConstants.UDEF_INE_PHONEBOOK:
                    case DDConstants.UDEF_OUTE_PHONEBOOK:
                        result_map.put(property_name, list_append((List<Map<String, Object>>) result_map.get(property_name), property_value));
                        break;
                    default:
                        throw new IOException(vl.toString());
                    }
                }
            } else {
                throw new IOException(vl.toString());
            }
        }
        context.write(NullWritable.get(), new Text(JSON.toJSONString(result_map)));
    }

    /**
     * Every input parameter should be identical.
     */
    private String identity_merge(String part_result, String element) throws IOException {
        if (part_result != null && !part_result.equals(element)) {
            throw new IOException("duplicate edge:" + part_result + "|" + element);
        } else {
            return element;
        }
    }
    private String minimum_time(String part_result, String element) {
        if (part_result != null && part_result.compareTo(element) < 0) {
            return part_result;
        } else {
            return element;
        }
    }
    private String maximum_time(String part_result, String element) {
        if (part_result != null && part_result.compareTo(element) > 0 ) {
            return part_result;
        } else {
            return element;
        }
    }
    private Integer minimum_integer(Integer part_result, Integer element) {
        if (part_result != null && part_result < element) {
            return part_result;
        } else {
            return element;
        }
    }
    private Integer maximum_integer(Integer part_result, Integer element) {
        if (part_result != null && part_result > element) {
            return part_result;
        } else {
            return element;
        }
    }
    private List<Map<String, String>> minimum_date(List<Map<String, String>> part_result, Map<String, Object> property_value) {
        if (part_result != null) {
            String old_value = part_result.get(0).get(DDConstants.PROPERTY_VALUE);
            String new_value = (String) property_value.get(DDConstants.PROPERTY_VALUE);
            if (old_value.compareTo(new_value) < 0) {
                return part_result;
            } else {
                part_result.get(0).put(DDConstants.PROPERTY_VALUE, new_value);
                return part_result;
            }
        } else {
            List<Map<String, String>> result = new ArrayList<>();
            result.add(new HashMap<>());
            result.get(0).put(DDConstants.PROPERTY_VALUE, (String) property_value.get(DDConstants.PROPERTY_VALUE));
            return result;
        }
    }
    private List<Map<String, String>> maximum_date(List<Map<String, String>> part_result, Map<String, String> element) {
        if (part_result != null) {
            String old_value = part_result.get(0).get(DDConstants.PROPERTY_VALUE);
            String new_value = element.get(DDConstants.PROPERTY_VALUE);
            if (old_value.compareTo(new_value) > 0) {
                return part_result;
            } else {
                part_result.get(0).put(DDConstants.PROPERTY_VALUE, new_value);
                return part_result;
            }
        } else {
            List<Map<String, String>> result = new ArrayList<>();
            result.add(new HashMap<>());
            result.get(0).put(DDConstants.PROPERTY_VALUE, element.get(DDConstants.PROPERTY_VALUE));
            return result;
        }
    }
    private List<Map<String, Integer>> final_passed_merge(List<Map<String, Integer>> part_result, Map<String, Object> property_value) {
        if (part_result != null) {
            Integer old_value = part_result.get(0).get(DDConstants.PROPERTY_VALUE);
            Integer new_value = Integer.parseInt((String) property_value.get(DDConstants.PROPERTY_VALUE));
            if (old_value > new_value) {
                return part_result;
            } else {
                part_result.get(0).put(DDConstants.PROPERTY_VALUE, new_value);
                return part_result;
            }
        } else {
            List<Map<String, Integer>> result = new ArrayList<>();
            result.add(new HashMap<>());
            result.get(0).put(DDConstants.PROPERTY_VALUE, Integer.parseInt((String) property_value.get(DDConstants.PROPERTY_VALUE)));
            return result;
        }
    }
    private List<Map<String, Object>> list_append(List<Map<String, Object>> part_result, final Map<String, Object> property_value) {
        if (part_result != null ) {
            part_result.add(property_value);
            return part_result;
        } else {
            List<Map<String, Object>> result = new ArrayList<>();
            result.add(property_value);
            return result;
        }
    }
    private Integer sum_weight(Integer part_result, Integer property_value) {
        if (part_result != null) {
            return part_result + property_value;
        } else {
            return property_value;
        }
    }
}
    
