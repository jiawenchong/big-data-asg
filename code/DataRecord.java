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
    }

    public String toString() {
        return speed + "," + travelTime + "," + borough;
    }

    public static DataRecord fromString(String str) {
        String[] fields = str.split(",");
        double speed = Double.parseDouble(fields[0]);
        double travelTime = Double.parseDouble(fields[1]);
        String borough = fields[2];
        return new DataRecord(speed, travelTime, borough);
    }

    public static List<DataRecord> takeRandomSample(List<DataRecord> dataRecords, int sampleSize) {
        return new ArrayList<>();
    }
}
