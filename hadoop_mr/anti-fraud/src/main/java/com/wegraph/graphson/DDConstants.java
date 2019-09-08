package com.wegraph.graphson;

public class DDConstants {
    /** first level separator */
    public final static String DELIMITER     = "\u0001";
    /** second level separator, used as a raw string */
    public final static String SEPARATOR     = "\u0002";
    /** minimum number of field for a edge information line */
    public final static int MIN_EDGE_FIELDS  = 6;
    /** minimum number of field for a vertex information line */
    public final static int MIN_VERTEX_FIELDS= 3;
    /** 'VERTEX' OR 'EDGE' identifier for a line */
    public final static int LINE_TYPE_I      = 0;
    public final static int FROM_VERTEX_I    = 1;
    public final static int TO_VERTEX_I      = 2;
    public final static int PROPERTY_NAME_I  = 0;            // label | relation_time | ...
    public final static int PROPERTY_VALUE_I = 1;            // value
    public final static String INFO_TYPE_K   = "INFO_TYPE";
    public final static String TYPE_EDGE_V   = "EDGE";       // value for PROPERTY_TYPE
    public final static String TYPE_VERTEX_V = "VERTEX";     // value for PROPERTY_TYPE
    
    public final static int VERTEX_LABEL_POS = 2;
    public final static int EDGE_LABEL_POS = 3;
    public final static int FROM_LABEL_POS = 4;
    public final static int TO_LABEL_POS = 5;
    
    /** named according to GraphSon format, do not modify! */
    public final static String TO_VERTEX_K   = "inV";
    /** named according to GraphSon format, do not modify! */
    public final static String FROM_VERTEX_K = "outV";
    /** predefined label for hive, will be changed to "label" */
    public final static String TO_LABEL_K    = "TO_LABEL";   
    /** predefined label for hive, will be changed to "label" */
    public final static String FROM_LABEL_K  = "FROM_LABEL";
    /** predefined label for hive, will be changed to "label" */
    public final static String EDGE_LABEL_K  = "EDGE_LABEL";
    /** identifier for vertex property value */
    public final static String PROPERTY_VALUE= "PROPERTY_VALUE";
    
    /** user defined property name for convert edge to vertex properties */
    public final static String UDEF_INE_BIG_VERTEX = "inE_big_vertex";
    public final static String UDEF_OUTE_BIG_VERTEX= "outE_big_vertex";
    public final static String UDEF_INE_PHONEBOOK = "inE_phonebook";
    public final static String UDEF_OUTE_PHONEBOOK = "outE_phonebook";
    public final static String UDEF_IS_BIG_VERTEX = "p_v_is_big_vertex";
    
    /** big vertices file number variable */
    public final static String BIG_VERTEX_FILE_NUMBER = "BIG_VERTEX_FILE_NUMBER";
    public final static String SHORT_PHONE_FILE_NUMBER = "SHORT_PHONE_FILE_NUMBER";
    /** big vertices file prefix */
    public final static String MAPREDUCE_DEFAULT_PREFIX = "part-r-";
    public final static String SPARK_DEFAULT_PREFIX = "part-";
    public final static String BIG_VERTEX_FILE_PREFIX = "big-vertex-r-";
    public final static String SHORT_PHONE_FILE_PREFIX = "short-phone-r-";

}
