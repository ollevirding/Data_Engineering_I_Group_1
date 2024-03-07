mport java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.InetAddress;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class ChunkProcessor {

    public static void main(String[] args) throws IOException, InterruptedException {
        // Configure HDFS connection
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://localhost:9000"); // Replace with your namenode address
        FileSystem fs = FileSystem.get(conf);

        // Define base path and data node ID extraction (modify as needed)
        Path chunkBasePath = new Path("/path/to/chunks/");
        String dataNodeId = InetAddress.getLocalHost().getHostName().split("-")[1]; // Assuming hostname format DN-<id>

        while (true) {
            boolean noMoreChunks = true; // Flag to check for remaining chunks
            for (Path chunkPath : fs.globStatus(new Path(chunkBasePath, "*"))) {
                noMoreChunks = false; // Reset flag upon finding chunks

                StringBuilder cleanedData = new StringBuilder();
                cleanedData.append("[\n");

                try (BufferedReader reader = new BufferedReader(new FileReader(fs.open(chunkPath).getPath()))) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        try {
                            // Validate JSON structure and extract features
                            JSONObject jsonObject = new JSONObject(line);
                            JSONObject filteredJsonObject = new JSONObject();
                            filteredJsonObject.put("match_id", jsonObject.get("match_id"));
                            filteredJsonObject.put("duration", jsonObject.get("duration"));
                            filteredJsonObject.put("game_mode", jsonObject.get("game_mode"));
                            line = filteredJsonObject.toString();
                            cleanedData.append(line).append(",\n");
                        } catch (JSONException e) {
                            // Handle invalid JSON lines (log, skip, etc.)
                            System.err.println("Error parsing JSON line: " + line + " (skipping)");
                        }
                    }
                    // Remove trailing comma and newline if necessary
                    if (cleanedData.length() > 2) {
                        cleanedData.delete(cleanedData.length() - 2, cleanedData.length());
                    }
                    cleanedData.append("\n]");
                } catch (IOException e) {
                    // Handle file reading errors (log, retry, etc.)
                    System.err.println("Error reading chunk: " + e.getMessage());
                }

                // Convert and send data as JSON to master node
                Path outputPath = new Path("/path/to/output/" + dataNodeId + "_" + chunkPath.getName() + ".json");
                try (OutputStreamWriter writer = new OutputStreamWriter(fs.create(outputPath))) {
                    writer.write(cleanedData.toString()); // No need for additional formatting
                } catch (IOException e) {
                    // Handle file writing errors (log, retry, etc.)
                    System.err.println("Error writing to output: " + e.getMessage());
                } finally {
                    // Ensure chunk removal even if exceptions occur
                    fs.delete(chunkPath);
                }
            }

            // Check for stopping the process (no more chunks)
            if (noMoreChunks) {
                break;
            }

            // Sleep before checking again
            TimeUnit.SECONDS.sleep(10);
        }

        // Close connection (in a finally block for guaranteed cleanup)
        fs.close();
    }
}