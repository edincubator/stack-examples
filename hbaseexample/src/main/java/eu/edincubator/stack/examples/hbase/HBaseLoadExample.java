package eu.edincubator.stack.examples.hbase;

import com.opencsv.CSVReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.io.StringReader;

public class HBaseLoadExample extends Configured implements Tool {


    public static class HBaseWriterMapper extends Mapper<Object, Text, ImmutableBytesWritable, Put> {

        private long checkpoint = 100;
        private long count = 0;

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // Extract state using opencsv library
            CSVReader reader = new CSVReader(new StringReader(value.toString()));
            String[] line;

            while ((line = reader.readNext()) != null) {
                // Check that current line is not CSV's header
                if (!line.equals("state")) {
                    context.setStatus("Creating row");
                    byte [] row = Bytes.toBytes(line[0]);
                    Put put = new Put(row);

                    // Insert info
                    byte [] family = Bytes.toBytes("info");

                    // name
                    byte [] qualifier = Bytes.toBytes("name");
                    byte [] hvalue = Bytes.toBytes(line[1]);
                    put.addColumn(family, qualifier, hvalue);

                    // neighborhood
                    qualifier = Bytes.toBytes("neighborhood");
                    hvalue = Bytes.toBytes(line[2]);
                    put.addColumn(family, qualifier, hvalue);

                    // address
                    qualifier = Bytes.toBytes("address");
                    hvalue = Bytes.toBytes(line[3]);
                    put.addColumn(family, qualifier, hvalue);

                    // city
                    qualifier = Bytes.toBytes("city");
                    hvalue = Bytes.toBytes(line[4]);
                    put.addColumn(family, qualifier, hvalue);

                    // state
                    qualifier = Bytes.toBytes("state");
                    hvalue = Bytes.toBytes(line[5]);
                    put.addColumn(family, qualifier, hvalue);

                    // postal_code
                    qualifier = Bytes.toBytes("postal_code");
                    hvalue = Bytes.toBytes(line[6]);
                    put.addColumn(family, qualifier, hvalue);

                    // latitude
                    qualifier = Bytes.toBytes("postal_code");
                    hvalue = Bytes.toBytes(line[7]);
                    put.addColumn(family, qualifier, hvalue);

                    // longitude
                    qualifier = Bytes.toBytes("longitude");
                    hvalue = Bytes.toBytes(line[8]);
                    put.addColumn(family, qualifier, hvalue);

                    // is_open
                    qualifier = Bytes.toBytes("is_open");
                    hvalue = Bytes.toBytes(line[11]);
                    put.addColumn(family, qualifier, hvalue);

                    // categories
                    qualifier = Bytes.toBytes("categories");
                    hvalue = Bytes.toBytes(line[12]);
                    put.addColumn(family, qualifier, hvalue);


                    // Insert stats
                    family = Bytes.toBytes("stats");

                    // stars
                    qualifier = Bytes.toBytes("stars");
                    hvalue = Bytes.toBytes(line[9]);
                    put.addColumn(family, qualifier, hvalue);

                    // review_count
                    qualifier = Bytes.toBytes("review_count");
                    hvalue = Bytes.toBytes(line[10]);
                    put.addColumn(family, qualifier, hvalue);

                    context.write(new ImmutableBytesWritable(row), put);

                    // Set status every checkpoint lines
                    if(++count % checkpoint == 0) {
                        context.setStatus("Emitting Put " + count);
                    }
                }
            }
        }
    }

    public int run(String[] otherArgs) throws Exception {
        Configuration conf = getConf();

        Job job = Job.getInstance(conf, "HBase load example");
        job.setJarByClass(HBaseLoadExample.class);

        FileInputFormat.setInputPaths(job, otherArgs[0]);
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(HBaseWriterMapper.class);

        TableMapReduceUtil.initTableReducerJob(
                otherArgs[1],
                null,
                job
        );
        job.setNumReduceTasks(0);

        return (job.waitForCompletion(true) ? 0 : 1);
    }

    public static void main(String [] args) throws Exception {
        int status = ToolRunner.run(HBaseConfiguration.create(), new HBaseLoadExample(), args);
        System.exit(status);
    }
}
