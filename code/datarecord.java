import java.util.ArrayList;
import java.util.List;

public class DataRecord {
    double speed;
    double travelTime;
    String borough;

    public DataRecord(double speed, double travelTime, String borough) {
        this.speed = speed;
        this.travelTime = travelTime;
        this.borough = borough;
   public String toString() {
        return speed + "," + travelTime + "," + borough;
    }
    }

    // Additional methods to convert to/from string representation (if required)
   public static DataRecord fromString(String str) {
        String[] fields = str.split(",");
        double speed = Double.parseDouble(fields[0]);
        double travelTime = Double.parseDouble(fields[1]);
        String borough = fields[2];
        return new DataRecord(speed, travelTime, borough);
    }

    // Implement this method based on your requirements or use any random sampling method
    public static List<DataRecord> takeRandomSample(List<DataRecord> dataRecords, int sampleSize) {
        // Your implementation here
        return new ArrayList<>();
    }
} 
