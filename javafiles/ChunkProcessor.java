import java.io.BufferedReader;
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
        conf.set("fs.defaultFS", "hdfs://de-i-1-master:9000");
        FileSystem fs = FileSystem.get(conf);

        // Define base path and data node ID extraction (modify as needed)
        Path chunkBasePath = new Path("/yasp-chunks_test/");
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
                            // Validate JSON structure and extract features for analytics
                            JSONObject jsonObject = new JSONObject(line);
                            JSONObject filteredJsonObject = new JSONObject();

                            // General
                            filteredJsonObject.put("match_id", jsonObject.get("match_id"));
                            filteredJsonObject.put("radiant_win", jsonObject.get("radiant_win"));

                            // Feature 1: First blood time
                            filteredJsonObject.put("first_blood_time", jsonObject.get("first_blood_time"));

                            // Feature 2: Hero selection and winning rate
                            JSONArray players = jsonObject.getJSONArray("players");
                            JSONArray heroIds = new JSONArray();
                            for (int i = 0; i < players.length(); i++) {
                                JSONObject player = players.getJSONObject(i);
                                heroIds.put(player.getInt("hero_id"));
                            }
                            filteredJsonObject.put("hero_ids", heroIds);

                            // Feature 3: Teamfights
                            JSONArray teamfights = jsonObject.getJSONArray("teamfights");
                            filteredJsonObject.put("teamfights", teamfights);

                            // Feature 5: Most played/banned hero
                            if(jsonObject.has("picks_bans")) {
                                filteredJsonObject.put("picks_bans", jsonObject.get("picks_bans"));
                            }

                            // Feature 6: Economic lead and win rate
                            JSONArray radiantGoldAdv = jsonObject.getJSONArray("radiant_gold_adv");
                            if (radiantGoldAdv.length() >= 10) {
                                filteredJsonObject.put("10_min_radiant_gold_adv", radiantGoldAdv.getInt(10));
                            }

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
                Path outputPath = new Path("/yasp-chunks_test_output/" + dataNodeId + "_" + chunkPath.getName() + ".json");
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
            TimeUnit.SECONDS.sleep(1);
        }

        // Close connection (in a finally block for guaranteed cleanup)
        fs.close();
    }
}