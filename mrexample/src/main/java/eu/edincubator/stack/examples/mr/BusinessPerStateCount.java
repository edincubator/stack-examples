package eu.edincubator.stack.examples.mr;

import com.opencsv.CSVReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.io.StringReader;

public class BusinessPerStateCount {

    public static class RowTokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // Extract state using opencsv library
            CSVReader reader = new CSVReader(new StringReader(value.toString()));
            String[] line;

            while ((line = reader.readNext()) != null) {
                // Write "one" for current state to context
                context.write(new Text(line[5]), one);
            }


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

    public static void main(String [] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "state count");
        job.setJarByClass(BusinessPerStateCount.class);

        job.setMapperClass(RowTokenizerMapper.class);
        job.setReducerClass(StateSumReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
