import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
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

import com.wegraph.graphson.DDConstants;
import com.wegraph.graphson.deduplicate.EdgeReducer;
import com.wegraph.graphson.deduplicate.FilterMapper;


/**
 * @author handongqing01
 * Converting big vertices to property, Merging repeated edges, and generating additional field about edge
 */
public class FilterDeduplicate extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {

        Job job = Job.getInstance(getConf());
        Configuration conf = job.getConfiguration();
        String[] otherArgs = new GenericOptionsParser(conf, args)
            .getRemainingArgs();
        if (otherArgs.length < 5) {
            System.err.println("Usage: VertexFilter reducer_number <big vertex folder path> <short phone folder path> <output path> [<input path>]");
            return -1;
        }
        String big_vertices_path = otherArgs[1];
        String short_phones_path = otherArgs[2];
        Path out_path = new Path(otherArgs[3]);
        FileOutputFormat.setOutputPath(job, out_path);
        for (int i=4; i<otherArgs.length; ++i) {
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
        
        // add big vertex data files
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path("hdfs://nameservice1" + big_vertices_path);
        FileStatus[] status = fs.listStatus(path);
        int i = 0;
        for (FileStatus fileStat: status) {
            if (fileStat.isFile()) {
                String fileName = fileStat.getPath().toString();
                DistributedCache.addCacheFile(
                        new URI("hdfs://nameservice1"
                                + big_vertices_path.replaceAll("/$", "")
                                + "/"
                                + fileName.substring(fileName.lastIndexOf("/") + 1)
                                + "#"
                                + DDConstants.BIG_VERTEX_FILE_PREFIX
                                + String.format("%05d", i)),
                        conf);
                i += 1;
            }
        }
        conf.setInt(DDConstants.BIG_VERTEX_FILE_NUMBER, i);
        
        // add short phone data files
        path = new Path("hdfs://nameservice1" + short_phones_path);
        status = fs.listStatus(path);
        i = 0;
        for (FileStatus fileStat: status) {
            if (fileStat.isFile()) {
                String fileName = fileStat.getPath().toString();
                DistributedCache.addCacheFile(
                        new URI("hdfs://nameservice1"
                                + short_phones_path.replaceAll("/$", "")
                                + "/"
                                + fileName.substring(fileName.lastIndexOf("/") + 1)
                                + "#"
                                + DDConstants.SHORT_PHONE_FILE_PREFIX
                                + String.format("%05d", i)),
                        conf);
                i += 1;    
            }
        }
        conf.setInt(DDConstants.SHORT_PHONE_FILE_NUMBER, i);
        
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        int res = ToolRunner.run(conf, new FilterDeduplicate(), args);
        System.exit(res);
    }

}
