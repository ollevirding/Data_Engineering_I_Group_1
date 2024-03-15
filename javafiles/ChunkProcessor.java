import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.InetAddress;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class ChunkProcessor {

    public static void main(String[] args) throws IOException, InterruptedException {
        // Configure HDFS connection
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://de-i-1-master:9000");
        FileSystem fs = FileSystem.get(conf);
        System.out.println("Connected to HDFS");

        // Define base path and data node ID extraction (modify as needed)
        Path chunkBasePath = new Path("yasp_chunk_10000/");
        String dataNodeId = InetAddress.getLocalHost().getHostName().split("-")[1]; // Assuming hostname format DN-<id>
        System.out.println("Chunk base path: " + chunkBasePath);
        System.out.println(InetAddress.getLocalHost().getHostName());
        System.out.println("Data node ID: " + dataNodeId);
        Path completeFilePath = new Path("job_status/COMPLETE");


        while (true) {
            System.out.println("ID:" + dataNodeId + "Checking for chunks...");
            boolean noMoreChunks = true; // Flag to check for remaining chunks
            for (FileStatus fileStatus : fs.globStatus(new Path(chunkBasePath, "*"))) {
                Path chunkPath = fileStatus.getPath();
                noMoreChunks = false; // Reset flag upon finding chunks
                if (chunkPath.getName().contains("_COPYING_")) {
                    // Skip Hadoop temporary files
                    continue;
                }
                System.out.println("ID:" + dataNodeId + "Processing chunk: " + chunkPath.getName());
                StringBuilder cleanedData = new StringBuilder();
                cleanedData.append("[\n");

                try (BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(chunkPath)))) {
                    String line;
                    int linecount = 1;
                    while ((line = reader.readLine()) != null) {
                        try {
                            // Validate JSON structure and extract features for analytics
                            int lineSize = line.length(); // For String
                            JSONObject jsonObject = new JSONObject(line);
                            JSONObject filteredJsonObject = new JSONObject();
                            // General
                            filteredJsonObject.put("match_id", jsonObject.get("match_id"));
                            filteredJsonObject.put("radiant_win", jsonObject.get("radiant_win"));
                            filteredJsonObject.put("duration", jsonObject.get("duration"));

                            JSONArray players = jsonObject.getJSONArray("players");
                            // initialize new players column
                            JSONArray newPlayers = new JSONArray();
                            for (int i = 0; i < players.length(); i++) {
                                JSONObject player = players.getJSONObject(i);
                                JSONObject newPlayer = new JSONObject();
                                if (player.has("account_id") && !player.isNull("account_id")) {
                                    newPlayer.put("account_id", player.getInt("account_id"));
                                } else {
                                    // Account ID is null, skip this player
                                    continue;
                                }
                                
                                newPlayer.put("kills", player.getInt("kills"));
                                newPlayer.put("deaths", player.getInt("deaths"));
                                newPlayer.put("assists, ", player.getInt("assists"));
                                newPlayers.put(newPlayer);
                            }
                            filteredJsonObject.put("players", newPlayers);

                            JSONArray radiantGoldAdv = jsonObject.getJSONArray("radiant_gold_adv");
                            filteredJsonObject.put("radiant_gold_adv", radiantGoldAdv);
                            JSONArray radiantXpAdv = jsonObject.getJSONArray("radiant_xp_adv");
                            filteredJsonObject.put("radiant_xp_adv", radiantXpAdv);

                            line = filteredJsonObject.toString();
                            cleanedData.append(line).append(",\n");
                            linecount++;

                        } catch (JSONException e) {
                            // Handle invalid JSON lines (log, skip, etc.)
                            if (line.length() > 10) {
                                System.err.println("Error parsing JSON line: " + e.getMessage());
                                e.printStackTrace(System.err);
                                System.err.println("Error parsing JSON line: " + line.substring(0, 10) + " (skipping)");
                            } else {
                                continue;
                            }
                            linecount++;
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
                Path outputPath = new Path(
                        "yasp_chunk_10000_output/" + chunkPath.getName() + ".json");
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
                System.out.println("No more chunks found, looking for COMPLETE file...");
                if (fs.exists(completeFilePath)) {
                    System.out.println("COMPLETE file found in HDFS. Terminating...");
                    break;
                } else {
                    System.out.println("COMPLETE file not found in HDFS. Waiting for more chunks...");
                }
            }

            // Sleep before checking again
            TimeUnit.SECONDS.sleep(5);
        }

        // Close connection (in a finally block for guaranteed cleanup)
        fs.close();
    }
}
