
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class LOFMapReduce {
    // Define constants to identify the output value types
    private static final Text DATA_RECORD_TAG = new Text("DATA_RECORD");
    private static final Text LOF_SCORE_TAG = new Text("LOF_SCORE");

    // Mapper class
    public static class LOFMapper extends Mapper<LongWritable, Text, Text, Text> {
        // Mapper implementation
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // Split the input value (CSV line) into its fields
            String[] fields = value.toString().split(",");

            // Assuming the CSV format is "speed,travelTime,borough"
            double speed = Double.parseDouble(fields[0]);
            double travelTime = Double.parseDouble(fields[1]);
            String borough = fields[2];

            // Create a DataRecord instance for the current data point
            DataRecord dataRecord = new DataRecord(speed, travelTime, borough);

            // Emit the data record to the reducer
            context.write(DATA_RECORD_TAG, new Text(dataRecord.toString()));
        }
    }

    // Reducer class
    public static class LOFReducer extends Reducer<Text, Text, Text, Text> {
        // Reducer implementation
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            if (key.equals(DATA_RECORD_TAG)) {
                // Process DataRecord instances
                List<DataRecord> dataRecords = new ArrayList<>();
                for (Text value : values) {
                    dataRecords.add(DataRecord.fromString(value.toString()));
                }

                // Take a random sample from dataRecords
                int sampleSize = 50000;
                List<DataRecord> dataSample = takeRandomSample(dataRecords, sampleSize);

                // Calculate LOF scores for the data sample
                double[] lofScores = calculateLOFScores(dataSample);

                // Emit LOF scores for the data sample
                for (int i = 0; i < dataSample.size(); i++) {
                    DataRecord dataRecord = dataSample.get(i);
                    context.write(new Text(dataRecord.toString()), new Text(String.valueOf(lofScores[i])));
                }
            }
        }
    }

    // Main method to set up and run the MapReduce job
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "LOFMapReduce");
        job.setJarByClass(LOFMapReduce.class);

        job.setMapperClass(LOFMapper.class);
        job.setReducerClass(LOFReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Specify the input and output paths
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Run the job and wait for completion
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
