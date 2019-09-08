package com.wegraph.analysis.filter;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.Text;

import com.wegraph.graphson.DDConstants;
import com.wegraph.graphson.deduplicate.EdgeMapper;

/**
 * @author handongqing01
 *
 */
public class FilterMapper extends EdgeMapper {
    private Set<String> valid_vertex_types = new HashSet<>(Arrays.asList(
            DDConstants.FROM_LABEL_K + DDConstants.SEPARATOR + "v_user_key",
            DDConstants.FROM_LABEL_K + DDConstants.SEPARATOR + "v_mobile",
            DDConstants.FROM_LABEL_K + DDConstants.SEPARATOR + "v_bankcard_no",
            DDConstants.FROM_LABEL_K + DDConstants.SEPARATOR + "v_id_no"));
    private Set<String> valid_edge_types = new HashSet<>(Arrays.asList(
            DDConstants.EDGE_LABEL_K + DDConstants.SEPARATOR + "e_register_mobile",
            DDConstants.EDGE_LABEL_K + DDConstants.SEPARATOR + "e_bankcard_no",
            DDConstants.EDGE_LABEL_K + DDConstants.SEPARATOR + "e_bankcard_mobile",
            DDConstants.EDGE_LABEL_K + DDConstants.SEPARATOR + "e_id_no"));

    @Override
    protected void map(Object key, Text value, Context context)
            throws IOException {
        List<String> tokens = new ArrayList<>();
        StringTokenizer itr = new StringTokenizer(value.toString(), DDConstants.DELIMITER);
        String rToken;
        while(itr.hasMoreTokens()) {
            rToken = itr.nextToken();
            if (!rToken.equals(NULL)) { tokens.add(rToken); }
        }

        if (tokens.get(DDConstants.LINE_TYPE_I).equals(DDConstants.TYPE_EDGE_V)) {
            if (valid_edge_types.contains(tokens.get(DDConstants.EDGE_LABEL_POS))) {
                parse_edge(context, tokens);
            }
        } else {
            if (valid_vertex_types.contains(tokens.get(DDConstants.VERTEX_LABEL_POS))){
                parse_vertex(context, tokens);
            }
        }

    }

}
