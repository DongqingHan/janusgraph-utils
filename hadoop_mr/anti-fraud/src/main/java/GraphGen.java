
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

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
    public static final String NUM_MAPS = "mapreduce.job.maps";
    public static final String NUM_REDUCES = "mapreduce.job.reduces";
    /**
     * An input format that assigns ranges of longs to each mapper.
     */
    static class RangeInputFormat extends InputFormat<LongWritable, NullWritable> {

        /**
         * An input split consisting of a range on numbers.
         */
        static class RangeInputSplit extends InputSplit implements Writable {
            long firstRow;
            long rowCount;

            public RangeInputSplit() {
            }

            public RangeInputSplit(long offset, long length) {
                firstRow = offset;
                rowCount = length;
            }

            public long getLength() {
                return 0;
            }

            public String[] getLocations() {
                return new String[]{};
            }

            public void readFields(DataInput in) throws IOException {
                firstRow = WritableUtils.readVLong(in);
                rowCount = WritableUtils.readVLong(in);
            }

            public void write(DataOutput out) throws IOException {
                WritableUtils.writeVLong(out, firstRow);
                WritableUtils.writeVLong(out, rowCount);
            }
        }

        /**
         * A record reader that will generate a range of numbers.
         */
        static class RangeRecordReader extends RecordReader<LongWritable, NullWritable> {
            long startRow;
            long finishedRows;
            long totalRows;
            LongWritable key = null;

            public RangeRecordReader() {
            }

            public void initialize(InputSplit split, TaskAttemptContext context) {
                startRow = ((RangeInputSplit) split).firstRow;
                finishedRows = 0;
                totalRows = ((RangeInputSplit) split).rowCount;
            }

            public void close() {
                // NOTHING
            }

            public LongWritable getCurrentKey() {
                return key;
            }

            public NullWritable getCurrentValue() {
                return NullWritable.get();
            }

            public float getProgress() {
                return finishedRows / (float) totalRows;
            }

            public boolean nextKeyValue() {
                if (key == null) {
                    key = new LongWritable();
                }
                if (finishedRows < totalRows) {
                    key.set(startRow + finishedRows);
                    finishedRows += 1;
                    return true;
                } else {
                    return false;
                }
            }

        }

        public RecordReader<LongWritable, NullWritable>
        createRecordReader(InputSplit split, TaskAttemptContext context) {
            return new RangeRecordReader();
        }

        /**
         * Create the desired number of splits, dividing the number of rows
         * between the mappers.
         */
        public List<InputSplit> getSplits(JobContext job) {
            long totalRows = getNumberOfRows(job);
            long numSplits = getNumberOfSplits(job);
//            LOG.info("Generating " + totalRows + " using " + numSplits);
            List<InputSplit> splits = new ArrayList<>();
            long currentRow = 0;
            for (int split = 0; split < numSplits; ++split) {
                long goal = (long) Math.ceil(totalRows * (double) (split + 1) / numSplits);
                splits.add(new RangeInputSplit(currentRow, goal - currentRow));
                currentRow = goal;
            }
            return splits;
        }

    }

    static long getNumberOfRows(JobContext job) {
        return job.getConfiguration().getLong(NUM_ROWS, 0);
    }

    static void setNumberOfRows(Job job, long numRows) {
        job.getConfiguration().setLong(NUM_ROWS, numRows);
    }

    static void setNumberOfSplits(Job job, long numSplits) {
        job.getConfiguration().setLong(NUM_SPLITS, numSplits);
    }

    static long getNumberOfSplits(JobContext job) {
        return job.getConfiguration().getLong(NUM_SPLITS, 1);
    }
    /**
     * The Mapper class that given a row number, will generate the appropriate
     * output line.
     */
    public static class GraphGenMappper
            extends Mapper<LongWritable, NullWritable, LongWritable, LongWritable> {

        private Random rnd = null;

        public void map(LongWritable row, NullWritable ignored,
                        Context context) throws IOException, InterruptedException {
            if (null == rnd) {
                rnd = new Random();
            }
            context.write(new LongWritable(rnd.nextInt()), row);
        }

        @Override
        public void cleanup(Context context) {
            ;
        }
    }

    private static void usage() {
        System.err.println("GraphGenerator <num rows> <output dir>");
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
        if (args.length != 2) {
            usage();
            return 2;
        }
        setNumberOfRows(job, parseHumanLong(args[0]));
        setNumberOfSplits(job, job.getConfiguration().getInt(NUM_MAPS, 1));
        Path outputDir = new Path(args[1]);
        FileOutputFormat.setOutputPath(job, outputDir);
        job.setJobName("GraphGenerator");
        job.setMapperClass(GraphGenMappper.class);
//        job.setNumReduceTasks(job.getConfiguration().getInt(NUM_REDUCES, 100));
        job.setNumReduceTasks(0);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(LongWritable.class);
        job.setInputFormatClass(RangeInputFormat.class);
//        job.setOutputFormatClass(TextOutputFormat.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new GraphGen(), args);
        System.exit(res);
    }
}
//public class GraphGenerator extends Configured implements Tool {
//    @Override
//    public int run(String[] args) throws Exception {
//
//        Job job = Job.getInstance(getConf());
//        Configuration conf = job.getConfiguration();
//        String[] otherArgs = new GenericOptionsParser(conf, args)
//            .getRemainingArgs();
//        if (otherArgs.length < 3) {
//            System.err.println("Usage: DataDecompress <reducer_number> <output path> [<input path>]");
//            return -1;
//        }
//        Path out_path = new Path(otherArgs[1]);
//        FileOutputFormat.setOutputPath(job, out_path);
//        for (int i=2; i<otherArgs.length; ++i) {
//            MultipleInputs.addInputPath(job, new Path(otherArgs[i]), TextInputFormat.class);
//        }
//
//        if (Integer.parseInt(otherArgs[0]) > 0) {
//            job.setMapperClass(IdentityMapper.class);
//            job.setReducerClass(IdentityReducer.class);
//            job.setMapOutputKeyClass(Text.class);
//            job.setMapOutputValueClass(NullWritable.class);
//            job.setOutputKeyClass(NullWritable.class);
//            job.setOutputValueClass(Text.class);
//        } else {
//            job.setMapperClass(IdentityMapper.class);
//        }
//        job.setNumReduceTasks(Integer.parseInt(otherArgs[0]));
//
//        return job.waitForCompletion(true) ? 0 : 1;
//    }
//
//    public static void main(String[] args) throws Exception {
//        Configuration conf = new Configuration();
//        int res = ToolRunner.run(conf, new GraphGenerator(), args);
//        System.exit(res);
//    }
//
//}
