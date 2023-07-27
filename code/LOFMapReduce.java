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
import java.util.Random;

public class LOFMapReduce {
    // Define constants to identify the output value types
    private static final Text DATA_RECORD_TAG = new Text("DATA_RECORD");
    private static final Text LOF_SCORE_TAG = new Text("LOF_SCORE");

    // Mapper
    public static class LOFMapper extends Mapper<LongWritable, Text, Text, Text> {
        private static final int SAMPLE_SIZE = 400000;
        private List<Text> sampledData = new ArrayList<>();

        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // Skip the header line
            if (key.get() == 0) {
                return;
            }

            // Split the input value (CSV line) into its fields
            String[] fields = value.toString().split(",");

            // Assuming the CSV format is "speed, travelTime, borough"
            double speed = Double.parseDouble(fields[0]);
            double travelTime = Double.parseDouble(fields[1]);
            String borough = fields[2];

            // Emit the data record with the borough as key and the values as text
            context.write(new Text(borough), value);

            // Add the data record to the sampled data list
            sampledData.add(value);
        }

        // take a random sample from the data records
        private List<Text> takeRandomSample() {
            if (sampledData.size() <= SAMPLE_SIZE) {
                return sampledData;
            }

            List<Text> sampledDataList = new ArrayList<>();
            Random random = new Random();

            for (int i = 0; i < SAMPLE_SIZE; i++) {
                int index = random.nextInt(sampledData.size());
                sampledDataList.add(sampledData.get(index));
                sampledData.remove(index);
            }

            return sampledDataList;
        }

        protected void cleanup(Context context) throws IOException, InterruptedException {
            // Take a random sample from the data records
            List<Text> dataSample = takeRandomSample();

            // Calculate LOF scores for the data sample
            double[] lofScores = calculateLOFScores(dataSample);

            // Emit LOF scores for the data sample
            int i = 0;
            for (Text dataRecord : dataSample) {
                // Use the line number as the identifier for each data point
                context.write(new Text("DATA_" + i), new Text(dataRecord.toString() + "," + lofScores[i]));
                i++;
            }
        }

        // LOF algorithm
       private double[] calculateLOFScores(List<Text> dataSample) {
    int k = 5; // The number of neighbors to consider in LOF calculation
    double[] lofScores = new double[dataSample.size()];

    // Convert data records to points with speed and travelTime attributes
    List<Point> points = new ArrayList<>();
    for (Text dataRecord : dataSample) {
        String[] fields = dataRecord.toString().split(",");
        double speed = Double.parseDouble(fields[0]);
        double travelTime = Double.parseDouble(fields[1]);
        Point point = new Point(speed, travelTime);
        points.add(point);
    }

    // Calculate LOF scores for each point
    for (int i = 0; i < points.size(); i++) {
        Point point = points.get(i);
        List<Double> distances = new ArrayList<>();

        // Calculate distances from the point to all other points
        for (int j = 0; j < points.size(); j++) {
            if (i != j) {
                Point otherPoint = points.get(j);
                double distance = calculateDistance(point, otherPoint);
                distances.add(distance);
            }
        }

        // Find k-nearest neighbors
        Collections.sort(distances);
        double kDistance = distances.get(k - 1);

        // Calculate reachability distances
        List<Double> reachabilityDistances = new ArrayList<>();
        for (double distance : distances) {
            double reachabilityDistance = Math.max(distance, kDistance);
            reachabilityDistances.add(reachabilityDistance);
        }

        // Calculate Local Reachability Density (LRD)
        double lrd = 0.0;
        for (double reachabilityDistance : reachabilityDistances) {
            lrd += reachabilityDistance;
        }
        lrd = k / lrd;

        // Calculate LOF score
        double lof = 0.0;
        for (double reachabilityDistance : reachabilityDistances) {
            int index = distances.indexOf(reachabilityDistance);
            lof += lrd / reachabilityDistances.get(index);
        }
        lof = lof / k;
        lofScores[i] = lof;
    }

    return lofScores;
}

private double calculateDistance(Point p1, Point p2) {
    double speedDiff = p1.speed - p2.speed;
    double travelTimeDiff = p1.travelTime - p2.travelTime;
    return Math.sqrt(speedDiff * speedDiff + travelTimeDiff * travelTimeDiff);
}

private static class Point {
    double speed;
    double travelTime;

    Point(double speed, double travelTime) {
        this.speed = speed;
        this.travelTime = travelTime;
    }
}


    // Reducer
    public static class LOFReducer extends Reducer<Text, Text, Text, Text> {
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            if (key.equals(DATA_RECORD_TAG)) {
                // Process DataRecord instances
                List<DataRecord> dataRecords = new ArrayList<>();
                List<Double> lofScores = new ArrayList<>();

                for (Text value : values) {
                    DataRecord dataRecord = DataRecord.fromString(value.toString());
                    dataRecords.add(dataRecord);

                    // Parse the LOF score from the value
                    String[] parts = value.toString().split(",");
                    double lofScore = Double.parseDouble(parts[3]);
                    lofScores.add(lofScore);
                }

                // Categorize LOF scores and count occurrences
                int countLe1 = 0;
                int countGt1 = 0;
                int countNa = 0;

                for (double lofScore : lofScores) {
                    if (Double.isNaN(lofScore)) {
                        countNa++;
                    } else if (lofScore <= 1.0) {
                        countLe1++;
                    } else {
                        countGt1++;
                    }
                }

                // Emit the final aggregation for the unique borough
                context.write(key, new Text("count_le1:" + countLe1 + ",count_gt1:" + countGt1 + ",count_na:" + countNa));
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
