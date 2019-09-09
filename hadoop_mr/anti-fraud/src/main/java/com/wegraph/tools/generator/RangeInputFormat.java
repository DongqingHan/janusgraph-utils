package com.wegraph.tools.generator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class RangeInputFormat extends InputFormat<LongWritable, NullWritable> {
    public static final String NUM_ROWS = "mapreduce.graphgen.num-rows";
    public static final String NUM_SPLITS = "mapreduce.graphgen.num-splits";
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
    static long getNumberOfSplits(JobContext job) {
        return job.getConfiguration().getLong(NUM_SPLITS, 1);
    }
    static long getNumberOfRows(JobContext job) {
        return job.getConfiguration().getLong(NUM_ROWS, 0);
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

