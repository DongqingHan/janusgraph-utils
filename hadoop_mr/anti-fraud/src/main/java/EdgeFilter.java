
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.wegraph.analysis.filter.FilterMapper;
import com.wegraph.graphson.deduplicate.EdgeReducer;

/**
 * created by handongqing on 2018/8/11.
 *     Merge repeated edges, and generating additional field about edge.
 */
public class EdgeFilter extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {

        Job job = Job.getInstance(getConf());
        Configuration conf = job.getConfiguration();
        String[] otherArgs = new GenericOptionsParser(conf, args)
                .getRemainingArgs();
        if (otherArgs.length < 3) {
            System.err.println("Usage: DeDuplicate reducer_number <output path> [<input path>,]");
            return -1;
        }
        Path out_path = new Path(otherArgs[1]);
        FileOutputFormat.setOutputPath(job, out_path);
        for (int i=2; i<otherArgs.length; ++i) {
            MultipleInputs.addInputPath(job, new Path(otherArgs[i]), TextInputFormat.class);
        }
        job.setMapperClass(FilterMapper.class);
        job.setReducerClass(EdgeReducer.class);
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
        int res = ToolRunner.run(conf, new EdgeFilter(), args);
        System.exit(res);
    }
}
