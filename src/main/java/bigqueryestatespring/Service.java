package bigqueryestatespring;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.*;

import java.io.FileInputStream;
import java.util.List;
import java.util.UUID;

public class Service {
    public static TableResult getDataByTopAndBottomArea(int bottom, int top) throws Exception {
        BigQuery bigquery = BigQueryOptions.newBuilder()
                .setCredentials(
                        ServiceAccountCredentials
                                .fromStream(new FileInputStream("/Users/rneviantsev/GridDynamics/serviceAccount/rockStartCred.json"))
                ).build().getService();

        QueryJobConfiguration queryConfig =
                QueryJobConfiguration.newBuilder(
                        "SELECT" +
                                "  operation, property_type, country_name, state_name, AVG(price) AS avg_price " +
                                "FROM" +
                                "  `properati-data-public.properties_ar.properties_rent_201501` " +
                                "GROUP BY operation, property_type, country_name, state_name")
                        .setUseLegacySql(false)
                        .build();

        JobId jobId = JobId.of(UUID.randomUUID().toString());
        Job queryJob = bigquery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());

        // Wait for the query to complete.
        queryJob = queryJob.waitFor();

        // Check for errors
        if (queryJob == null) {
            throw new RuntimeException("Job no longer exists");
        } else if (queryJob.getStatus().getError() != null) {
            throw new RuntimeException(queryJob.getStatus().getError().toString());
        }

        return queryJob.getQueryResults();
    }

    public static Node createTree(TableResult tableResult) {
        Node root = new Node("root");
        List<String> columnNames = List.of("operation", "property_type", "country_name", "state_name",  "avg_price");

        for (FieldValueList row : tableResult.iterateAll()) {
            createBranch(root, columnNames, row);
//            String operation = row.get("operation").getStringValue();
//            String country_name = row.get("country_name").getStringValue();
//            String avg_price = row.get("avg_price").getStringValue();
//            System.out.printf("operation: %s country_name: %s avg_price: %s%n", operation, country_name, avg_price);
        }

        return root;
    }

    private static void createBranch(Node current, List<String> columnNames, FieldValueList row) {
        boolean exists;
        String columnValue;
        // Go through all column names except for the last one
        for (String columnName: columnNames.subList(0, columnNames.size() - 1)) {
            exists = false;
            columnValue = row.get(columnName).getStringValue();
            for (Node node: current.getChildren()) {
                // If node with the same data already exists - make it 'current'
                if (node.checkEqualData(columnValue)) {
                    current = node;
                    exists = true;
                    break;
                }
            }
            // If node with the same data doesn't exist - create it and then make it 'current'
            if (!exists) {
                Node newNode = new Node(columnValue);
                current.addChild(newNode);
                current = newNode;
            }
        }
        // Last element processes outside 'for' because so we won't need to initialize list of children
        columnValue = row.get(columnNames.get(columnNames.size() - 1)).getStringValue();
        current.addChild(new Node(columnValue));
    }

    public static String constructJson(Node root) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode rootNode = mapper.valueToTree(root);

        return mapper.writeValueAsString(rootNode);
    }

    public static String getEstates() throws Exception {
        TableResult result = Service.getDataByTopAndBottomArea(0, 999999);
        Node root = Service.createTree(result);
        return Service.constructJson(root);
    }
}
