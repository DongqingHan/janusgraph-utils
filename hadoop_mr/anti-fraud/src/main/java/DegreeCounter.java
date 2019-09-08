import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.wegraph.analysis.collector.DegreeMapper;
import com.wegraph.analysis.collector.DegreeReducer;


/**
 * @author handongqing01
 * Computing vertex degree.
 */
public class DegreeCounter extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {

        Job job = Job.getInstance(getConf());
        Configuration conf = job.getConfiguration();
        String[] otherArgs = new GenericOptionsParser(conf, args)
            .getRemainingArgs();
        if (otherArgs.length < 4) {
            System.err.println("Usage: DegreeCounter reducer_number threshold <output path> [<input paths>]");
            return -1;
        }
        conf.setInt("THRESHOLD", Integer.parseInt(otherArgs[1]));
        
        Path out_path = new Path(otherArgs[2]);
        FileOutputFormat.setOutputPath(job, out_path);
        for (int i=3; i<otherArgs.length; ++i) {
            MultipleInputs.addInputPath(job, new Path(otherArgs[i]), TextInputFormat.class);
        }
        
        job.setMapperClass(DegreeMapper.class);
        job.setReducerClass(DegreeReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        //reduce number corresponding to the result file number
        job.setNumReduceTasks(Integer.parseInt(otherArgs[0]));
        
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        int res = ToolRunner.run(conf, new DegreeCounter(), args);
        System.exit(res);
    }

}
