package util;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.Path;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;

public class ElasticUtils {

    private static final ObjectMapper om = new ObjectMapper();

    private static int bulkId = 1;

    public void loadBulksToElastic(Path srcDir) throws IOException {

        TransportClient client = new PreBuiltTransportClient(Settings.EMPTY)
                .addTransportAddress(new TransportAddress(InetAddress.getByName("localhost"), 9300));

        Files.walk(srcDir, 1)//
                .filter(v -> Files.isRegularFile(v))//
                .forEach(v -> {
                    BulkRequestBuilder bulkRequest = client.prepareBulk();
                    try {
                        byte[] fileBytes = IOUtils.readFile(v).getBytes();
                        bulkRequest.add(fileBytes, 0, fileBytes.length, XContentType.JSON);
                        bulkRequest.execute();

                        Thread.currentThread().sleep(5000);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                });

        client.close();
    }

    public void prepareBulkJsons(Path srcDir, Path destDir) throws IOException {
        Files.walk(srcDir, 1)//
                .filter(v -> Files.isRegularFile(v))//
                .forEach(v -> {
                    try {
                        prepareBulkJson(v, destDir);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                });
    }

    public void prepareBulkJson(Path srcPath, Path destPath) throws IOException {
        ArrayNode an = (ArrayNode) om.readTree(IOUtils.readFile(srcPath));
        StringBuilder sb = new StringBuilder();

        String bulkRow = "{ \"index\" : { \"_index\" : \"title\", \"_type\" : \"_doc\", \"_id\" : \"REPLACE\" } }";

        for (JsonNode n : an) {
            sb.append(bulkRow.replaceAll("REPLACE", String.valueOf(bulkId++)));
            sb.append("\n");
            sb.append(n.toString());
            sb.append("\n");
        }

        String fileName = srcPath.getFileName().toString().replaceAll(".json", "");
        IOUtils.writeToFile(destPath.resolve(fileName + "_bulk.json"), sb.toString());
    }
}
