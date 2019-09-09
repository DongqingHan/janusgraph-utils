
import com.wegraph.tools.generator.GraphGenMapper;
import com.wegraph.tools.generator.RangeInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * Generate a Erdos-Rendy random graph.
 * The user specifies the number of edges and the output directory and this
 * class runs a map/reduce program to generate the edge list.
 * The format of the data is:
 * <ul>
 * <li>(10 bytes key) (10 bytes rowid) (78 bytes filler) \r \n
 * <li>The keys are random characters from the set ' ' .. '~'.
 * <li>The rowid is the right justified row id as a int.
 * <li>The filler consists of 7 runs of 10 characters from 'A' to 'Z'.
 * </ul>
 *
 * <p>
 * To run the program:
 * <b>bin/hadoop jar hadoop-*-examples.jar GraphGen 10000000000 in-dir</b>
 */
public class GraphGen extends Configured implements Tool {
//    private static final Log LOG = LogFactory.getLog(TeraSort.class);

    public static final String NUM_ROWS = "mapreduce.graphgen.num-rows";
    public static final String NUM_SPLITS = "mapreduce.graphgen.num-splits";
    public static final String NUM_REDUCES = "mapreduce.job.reduces";
    public static final String NUM_MAPS = "mapreduce.job.maps";

    public static final String NUM_VERTICES = "graphgen.num-vertices";
    public static final String NUM_EDGES = "graphgen.num-edges";
    /**
     * An input format that assigns ranges of longs to each mapper.
     */

    static void setNumberOfRows(Job job, long numRows) {
        job.getConfiguration().setLong(NUM_ROWS, numRows);
    }

    static void setNumberOfSplits(Job job, long numSplits) {
        if (job.getConfiguration().getLong(NUM_SPLITS, 0) > 0) {
            // NOTHING
        } else {
            job.getConfiguration().setLong(NUM_SPLITS, numSplits);
        }
    }

    static void setNumVertices(Job job, long numVertices) {
        job.getConfiguration().setLong(NUM_VERTICES, numVertices);
    }

    static long computeNumOfRows(long numVertices, long numEdges) {
        return (long) (numEdges + Math.pow(numEdges / (numVertices * 1.41421256), 2.0));
    }

    private static void usage() {
        System.err.println("GraphGenerator <num vertices> <num edges> <output dir>");
    }

    /**
     * Parse a number that optionally has a postfix that denotes a base.
     *
     * @param str an string integer with an option base {k,m,b,t}.
     * @return the expanded value
     */
    private static long parseHumanLong(String str) {
        char tail = str.charAt(str.length() - 1);
        long base = 1;
        switch (tail) {
            case 't':
                base *= 1000 * 1000 * 1000 * 1000;
                break;
            case 'b':
                base *= 1000 * 1000 * 1000;
                break;
            case 'm':
                base *= 1000 * 1000;
                break;
            case 'k':
                base *= 1000;
                break;
            default:
        }
        if (base != 1) {
            str = str.substring(0, str.length() - 1);
        }
        return Long.parseLong(str) * base;
    }

    /**
     * @param args the cli arguments
     */
    public int run(String[] args)
            throws IOException, InterruptedException, ClassNotFoundException {
        Job job = Job.getInstance(getConf());
        if (args.length != 3) {
            usage();
            return 3;
        }
        setNumVertices(job, parseHumanLong(args[0]));
        setNumberOfRows(job, computeNumOfRows(parseHumanLong(args[0]), parseHumanLong(args[1])));
        setNumberOfSplits(job, job.getConfiguration().getInt(NUM_MAPS, 1));
        Path outputDir = new Path(args[2]);
        FileOutputFormat.setOutputPath(job, outputDir);
        job.setJobName("GraphGenerator");
        job.setMapperClass(GraphGenMapper.class);
//        job.setReducerClass(GraphGenReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
//        job.setOutputKeyClass(LongWritable.class);
//        job.setOutputValueClass(LongWritable.class);
        job.setInputFormatClass(RangeInputFormat.class);
//        job.setNumReduceTasks(job.getConfiguration().getInt(NUM_REDUCES, 100));
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new GraphGen(), args);
        System.exit(res);
    }
}