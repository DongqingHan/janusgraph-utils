package com.wegraph.graphson.deduplicate;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import java.io.*;
import java.util.*;

import com.wegraph.graphson.DDConstants;
/**
 * @author handongqing01
 *
 */
public class FilterMapper extends EdgeMapper {
    private Set<String> big_vertices = new HashSet<>();
    private Set<String> short_phones = new HashSet<>();

    @Override
    protected void setup(Context context)
            throws IOException, InterruptedException {
        super.setup(context);
        
        // loading big vertex list and short phone list from cache files.
        Path[] localCacheFiles = context.getLocalCacheFiles();
        if (localCacheFiles != null && localCacheFiles.length > 0) {
            for (int i=0; i < context.getConfiguration().getInt(DDConstants.BIG_VERTEX_FILE_NUMBER, 0); ++i) {
                try {
                    BufferedReader reader = new BufferedReader(new FileReader(new File("./" + DDConstants.BIG_VERTEX_FILE_PREFIX + String.format("%05d", i))));
                    String line;
                    while ((line = reader.readLine()) != null ) {
                        big_vertices.add(line.trim().split(DDConstants.DELIMITER)[0]);
                    }
                    reader.close();
                } catch (FileNotFoundException e) {
                    throw new RuntimeException("FileNotFoundException for file:" + String.format("%05d", i));
                } catch (IOException e) {
                    throw new RuntimeException("IOException for file:" + String.format("%05d", i));
                }
            }
            
            for (int i=0; i < context.getConfiguration().getInt(DDConstants.SHORT_PHONE_FILE_NUMBER, 0); ++i) {
                try {
                    BufferedReader reader = new BufferedReader(new FileReader(new File("./" + DDConstants.SHORT_PHONE_FILE_PREFIX + String.format("%05d", i))));
                    String line;
                    while ((line = reader.readLine()) != null ) {
                        short_phones.add(line.trim().split(DDConstants.DELIMITER)[0]);
                    }
                    reader.close();
                } catch (FileNotFoundException e) {
                    throw new RuntimeException("FileNotFoundException for file:" + String.format("%05d", i));
                } catch (IOException e) {
                    throw new RuntimeException("IOException for file:" + String.format("%05d", i));
                }
            }
        } else {
            throw new RuntimeException("Cached files not found");
        }
      }
    
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
            if (short_phones.contains(tokens.get(DDConstants.FROM_VERTEX_I))
                    || short_phones.contains(tokens.get(DDConstants.TO_VERTEX_I))) {
                return;
            }

            boolean from_vertex_is_big = big_vertices.contains(tokens.get(DDConstants.FROM_VERTEX_I));
            boolean to_vertex_is_big = big_vertices.contains(tokens.get(DDConstants.TO_VERTEX_I));
            
            // check whether converting edge to vertex property.
            if ((from_vertex_is_big ^ to_vertex_is_big) 
                    && (tokens.get(DDConstants.EDGE_LABEL_POS).equals("EDGE_LABEL" + DDConstants.SEPARATOR + "e_call")
                            || tokens.get(DDConstants.EDGE_LABEL_POS).equals("EDGE_LABEL" + DDConstants.SEPARATOR + "e_phonebook"))) {
                parse_vertex(context, edgeToProperty(tokens, to_vertex_is_big));
                // construct the vertex property 'p_v_is_big_vertex' for both from vertex and to vertex.
                if (from_vertex_is_big) {
                    List<String> from_tokens = new ArrayList<>();
                    from_tokens.add(DDConstants.TYPE_VERTEX_V);
                    from_tokens.add(tokens.get(DDConstants.FROM_VERTEX_I));
                    from_tokens.add(tokens.get(DDConstants.FROM_LABEL_POS));
                    from_tokens.add(DDConstants.UDEF_IS_BIG_VERTEX + DDConstants.SEPARATOR + 1);
                    parse_vertex(context, from_tokens);
                }
                if (to_vertex_is_big) {
                    List<String> to_tokens = new ArrayList<>();
                    to_tokens.add(DDConstants.TYPE_VERTEX_V);
                    to_tokens.add(tokens.get(DDConstants.TO_VERTEX_I));
                    to_tokens.add(tokens.get(DDConstants.TO_LABEL_POS));
                    to_tokens.add(DDConstants.UDEF_IS_BIG_VERTEX + DDConstants.SEPARATOR + 1);
                    parse_vertex(context, to_tokens);
                }
            } else {
                parse_edge(context, tokens);
            }
        } else {
            parse_vertex(context, tokens);
            
        }
        
    }
    
    /**
     * Convert edge with big vertex into vertex property.
     * @param tokens parsed edge tokens.
     * @param drop_to_vertex true if to_vertex is big vertex else false.
     * @return parsed tokens representing a vertex property.
     * @throws IOException 
     */
    protected List<String> edgeToProperty(List<String> tokens, boolean drop_to_vertex) throws IOException {
        List<String> result_tokens = new ArrayList<>();
        for (int i=0; i <= DDConstants.TO_LABEL_POS; ++i) {
            switch (i) {
            case DDConstants.LINE_TYPE_I:
                result_tokens.add(DDConstants.TYPE_VERTEX_V);
                break;
            case DDConstants.FROM_VERTEX_I:
                if (drop_to_vertex) { result_tokens.add(tokens.get(DDConstants.FROM_VERTEX_I)); }
                break;
            case DDConstants.TO_VERTEX_I:
                if (!drop_to_vertex) { result_tokens.add(tokens.get(DDConstants.TO_VERTEX_I)); }
                break;
            case DDConstants.FROM_LABEL_POS:
                // add vertex label property
                if (drop_to_vertex) { result_tokens.add(tokens.get(DDConstants.FROM_LABEL_POS)); }
                break;
            case DDConstants.TO_LABEL_POS:
                // add vertex label property
                if (!drop_to_vertex) {
                    // change 'TO_LABEL|v_mobile' to 'FROM_LABEL|v_mobile'
                    String[] property_pair = tokens.get(DDConstants.TO_LABEL_POS).split(DDConstants.SEPARATOR);
                    result_tokens.add(DDConstants.FROM_LABEL_K + DDConstants.SEPARATOR + property_pair[DDConstants.PROPERTY_VALUE_I]);
                }
                break;
            default:
                break;
            }
        }
        
        // construct and add big vertex property
        String inE_key;
        String outE_key;
        if (tokens.get(DDConstants.EDGE_LABEL_POS).equals("EDGE_LABEL" + DDConstants.SEPARATOR + "e_call")) {
            inE_key = DDConstants.UDEF_INE_BIG_VERTEX;
            outE_key = DDConstants.UDEF_OUTE_BIG_VERTEX;
        } else if (tokens.get(DDConstants.EDGE_LABEL_POS).equals("EDGE_LABEL" + DDConstants.SEPARATOR + "e_phonebook")) {
            inE_key = DDConstants.UDEF_INE_PHONEBOOK;
            outE_key = DDConstants.UDEF_OUTE_PHONEBOOK;
        } else {
            throw new IOException(tokens.toString());
        }
        String property = (drop_to_vertex ? outE_key : inE_key)
                          + DDConstants.SEPARATOR
                          + (drop_to_vertex ? tokens.get(DDConstants.TO_VERTEX_I) : tokens.get(DDConstants.FROM_VERTEX_I));
        for (int i=DDConstants.TO_LABEL_POS + 1; i < tokens.size(); ++i) {
            property += (DDConstants.SEPARATOR + tokens.get(i));
        }
        result_tokens.add(property);

        return result_tokens;
    }

}
