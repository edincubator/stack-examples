package eu.edincubator.stack.examples.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class HBaseReadExample extends Configured implements Tool {

    public static class HBaseReadMapper extends TableMapper<Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);

        public void map(ImmutableBytesWritable row, Result value, Context context) throws IOException, InterruptedException {
            byte[] cell = value.getValue(Bytes.toBytes("info"), Bytes.toBytes("state"));
            context.write(new Text(Bytes.toString(cell)), one);
        }
    }

    public static class StateSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            // For each state coincidence, +one
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);

            // Return the state and the number of appearances.
            context.write(key, result);
        }
    }


    public int run(String[] otherArgs) throws Exception {
        Configuration conf = getConf();

        Job job = Job.getInstance(conf, "HBase read example");
        job.setJarByClass(HBaseReadExample.class);

        Scan scan = new Scan();
        scan.setCaching(500);
        scan.setCacheBlocks(false);

        TableMapReduceUtil.initTableMapperJob(
                otherArgs[0],
                scan,
                HBaseReadMapper.class,
                Text.class,
                IntWritable.class,
                job
        );

        job.setReducerClass(StateSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        return (job.waitForCompletion(true) ? 0 : 1);
    }

    public static void main(String [] args) throws Exception {
        int status = ToolRunner.run(HBaseConfiguration.create(), new HBaseReadExample(), args);
        System.exit(status);
    }
}
