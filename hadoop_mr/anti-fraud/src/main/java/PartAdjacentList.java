
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import com.wegraph.graphson.ajacentlist.PartMapper;
import com.wegraph.graphson.ajacentlist.PartReducer;

/**
 * 
 * created by handongqing on 2018/5/31
 * generate graphSon.
 */
public class PartAdjacentList extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {

        Job job = Job.getInstance(getConf());
        Configuration conf = job.getConfiguration();
        String[] otherArgs = new GenericOptionsParser(conf, args)
            .getRemainingArgs();
        if (otherArgs.length != 3) {
            System.err.println("Usage: PartAdjacentList reducer_number <intpu path> <output path>");
            return -1;
        }
        Path in_path = new Path(otherArgs[1]);
        Path out_path = new Path(otherArgs[2]);
        FileInputFormat.setInputPaths(job, in_path);
        FileOutputFormat.setOutputPath(job, out_path);
        //job.setJarByClass(PartAdjacentList.class);
        job.setMapperClass(PartMapper.class);
        job.setReducerClass(PartReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        //reduce number corresponding to the result file number
        job.setNumReduceTasks(Integer.parseInt(otherArgs[0]));
        
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        int res = ToolRunner.run(conf, new PartAdjacentList(), args);
        System.exit(res);
    }


}
