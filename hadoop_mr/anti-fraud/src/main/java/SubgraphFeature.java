import java.net.URI;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.wegraph.analysis.oltp.UserProfileMapper;
import com.wegraph.analysis.oltp.UserProfileReducer;
/**
 * @author handongqing01
 * Generating features.
 */
public class SubgraphFeature extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {

        Job job = Job.getInstance(getConf());
        Configuration conf = job.getConfiguration();
        String[] otherArgs = new GenericOptionsParser(conf, args)
            .getRemainingArgs();
        if (otherArgs.length < 3) {
            System.err.println("Usage: SubgraphFeature graph_configuration_fie <intput path> <output path>");
            return -1;
        }

        Path in_path = new Path(otherArgs[1]);
        Path out_path = new Path(otherArgs[2]);
        FileInputFormat.setInputPaths(job, in_path);
        FileOutputFormat.setOutputPath(job, out_path);
        job.setMapperClass(UserProfileMapper.class);
        job.setReducerClass(UserProfileReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        String fileName = otherArgs[0];
        DistributedCache.addCacheFile(
                new URI("hdfs://nameservice1"
                        + fileName
                        + "#"
                        + fileName.substring(fileName.lastIndexOf("/") + 1)),
                conf);
        conf.set("graph_configuration_file", fileName.substring(fileName.lastIndexOf("/") + 1));
        job.getConfiguration().set("mapreduce.job.maps", "100");
        job.setNumReduceTasks(10);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        int res = ToolRunner.run(conf, new SubgraphFeature(), args);
        System.exit(res);
    }

}
