package bigqueryestatespring.services;

import bigqueryestatespring.exceptionMessages.ExceptionMessage;
import bigqueryestatespring.nodes.Node;
import bigqueryestatespring.nodes.NodeWithChildren;
import bigqueryestatespring.nodes.PriceNode;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.*;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.UUID;

public class Service {
    public static BigQuery getBigQueryInstance() {
        try {
            return BigQueryOptions.newBuilder()
                    .setCredentials(
                            ServiceAccountCredentials
                                    .fromStream(new FileInputStream("/Users/rneviantsev/GridDynamics/serviceAccount/rockStartCred.json"))
                    ).build().getService();
        } catch (FileNotFoundException ex) {
            ex.printStackTrace();
            throw new RuntimeException(ExceptionMessage.CREDENTIALS_FILE_NOT_FOUND);
        } catch (IOException ex) {
            ex.printStackTrace();
            throw new RuntimeException(ExceptionMessage.EXCEPTION_WHILE_READING_CREDENTIALS);
        }
    }

    public static QueryJobConfiguration getQueryJobConfiguration(int bottom, int top) {
        return QueryJobConfiguration.newBuilder(
                "SELECT operation, property_type, country_name, state_name, AVG(price) AS avg_price " +
                        "FROM `properati-data-public.properties_ar.properties_rent_201501` " +
                        "WHERE (surface_covered_in_m2 >= " + bottom + " AND surface_covered_in_m2 <= " + top + ") " +
                        "OR (surface_covered_in_m2 IS NULL)" +
                        "GROUP BY operation, property_type, country_name, state_name ")
                .setUseLegacySql(false)
                .build();
    }

    public static TableResult getTableResultOfEstates(int bottom, int top) {
        BigQuery bigquery = getBigQueryInstance();

        QueryJobConfiguration queryConfig = getQueryJobConfiguration(bottom, top);

        JobId jobId = JobId.of(UUID.randomUUID().toString());
        Job queryJob = bigquery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());

        for (int i = 0; i < 5; i++) {
            try {
                // Wait for the query to complete.
                queryJob = queryJob.waitFor();

//                // Check for errors
//                if (queryJob == null) {
//                    throw new RuntimeException(ExceptionMessage.JOB_NO_LONGER_EXISTS);
//                } else if (queryJob.getStatus().getError() != null) {
//                    throw new RuntimeException(queryJob.getStatus().getError().toString());
//                }

                return queryJob.getQueryResults();
            } catch (InterruptedException ex) {
                ex.printStackTrace();
            }
        }
        throw new RuntimeException(ExceptionMessage.ERROR_WHILE_PROCESSING_QUERY);
    }

    public static Node createTree(TableResult tableResult) {
        NodeWithChildren root = new NodeWithChildren("");
        List<String> columnNames = List.of("operation", "property_type", "country_name", "state_name",  "avg_price");

        tableResult.iterateAll().forEach(row -> createBranch(root, columnNames, row));

        return root;
    }

    private static void createBranch(NodeWithChildren current, List<String> columnNames, FieldValueList row) {
        boolean exists;
        String columnValue;
        // Go through all column names except for the last one
        for (String columnName: columnNames.subList(0, columnNames.size() - 1)) {
            exists = false;
            columnValue = row.get(columnName).getStringValue();
            for (Node node: current.getChildren()) {
                // If node with the same data already exists - make it 'current'
                if (node.checkEqualData(columnValue)) {
                    current = (NodeWithChildren) node;
                    exists = true;
                    break;
                }
            }
            // If node with the same data doesn't exist - create it and then make it 'current'
            if (!exists) {
                NodeWithChildren newNode = new NodeWithChildren(columnValue);
                current.addChild(newNode);
                current = newNode;
            }
        }
        // Last element is processed outside 'for' because we want to create different type of Node
        columnValue = row.get(columnNames.get(columnNames.size() - 1)).getStringValue();
        current.addChild(new PriceNode(columnValue));
    }

    public static Object constructJson(Node root) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            String json = mapper.writeValueAsString(root);

            return mapper.readValue(json, Object.class);
        } catch (JsonProcessingException ex) {
            ex.printStackTrace();
        }
        return null;
    }

    public static Object getEstates(int bottom, int top) {
        TableResult result = getTableResultOfEstates(bottom, top);

        Node root = createTree(result);
        return Service.constructJson(root);
    }
}
