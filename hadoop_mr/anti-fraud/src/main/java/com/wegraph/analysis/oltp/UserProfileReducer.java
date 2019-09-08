package com.wegraph.analysis.oltp;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.alibaba.fastjson.JSON;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;



/**
 * @author handongqing01
 */

public  class UserProfileReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        Map<String, Object> res = new HashMap<>();
        String max_time = "";
        for (Text value: values){
            Map<String, Object> passed = JSON.parseObject(value.toString());
            for(Map.Entry<String, Object> entry: passed.entrySet()){
                if ("time".compareTo(entry.getKey()) ==  0 && max_time.compareTo((String) entry.getValue()) < 0) {
                    max_time = (String) entry.getValue();
                } else {
                    res.put(entry.getKey(), entry.getValue());
                }
            }
        }
        res.put("time", max_time);
        context.write(key, new Text(JSON.toJSONString(res)));
    }
}

