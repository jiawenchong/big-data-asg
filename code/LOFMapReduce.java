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

    // Mapper 
    public static class LOFMapper extends Mapper<LongWritable, Text, Text, Text> {
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // Skip the header line
            if (key.get() == 0) {
                return;
            }

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

    // Reducer
    public static class LOFReducer extends Reducer<Text, Text, Text, Text> {
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            if (key.equals(DATA_RECORD_TAG)) {
                // Process DataRecord instances
                List<DataRecord> dataRecords = new ArrayList<>();
                List<Double> lofScores = new ArrayList<>();
                List<String> boroughs = new ArrayList<>();

                for (Text value : values) {
                    DataRecord dataRecord = DataRecord.fromString(value.toString());
                    dataRecords.add(dataRecord);

                    // Assume LOF scores and boroughs are printed by the Python script
                    String[] parts = value.toString().split(",");
                    double lofScore = Double.parseDouble(parts[3]);
                    String borough = parts[4];

                    lofScores.add(lofScore);
                    boroughs.add(borough);
                }

                // Perform the final aggregation
                for (int i = 0; i < dataRecords.size(); i++) {
                    String borough = boroughs.get(i);
                    double lofScore = lofScores.get(i);

                    // Emit the data record along with its borough and LOF score
                    context.write(new Text(borough), new Text(dataRecords.get(i).toString() + "," + lofScore));
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
