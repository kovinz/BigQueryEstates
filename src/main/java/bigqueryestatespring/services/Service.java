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
    private static String PATH_TO_CREDENTIALS = "/Users/rneviantsev/GridDynamics/serviceAccount/rockStartCred.json";
    private List<String> columnNames;
    private OperationType operationType;
    private String alias;

    public Service(List<String> columnNames, OperationType operationType) {
        this.columnNames = columnNames;
        this.operationType = operationType;
        this.alias = operationType.getStringValue().toLowerCase() + '_' + columnNames.get(columnNames.size() - 1);
    }

    /**
     * Gets bigQuery instance with credentials for estates project
     *
     * @return bigQuery instance
     */
    private BigQuery getBigQueryInstance() {
        try {
            return BigQueryOptions.newBuilder()
                    .setCredentials(
                            ServiceAccountCredentials
                                    .fromStream(new FileInputStream(PATH_TO_CREDENTIALS))
                    ).build().getService();
        } catch (FileNotFoundException ex) {
            ex.printStackTrace();
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        return null;
    }

    private QueryJobConfiguration getQueryJobConfiguration(int bottom, int top) {
        StringBuilder queryStrBuilder = new StringBuilder("SELECT ");
        for (String columnName: columnNames.subList(0, columnNames.size() - 1)) {
            queryStrBuilder.append(columnName).append(", ");
        }
        queryStrBuilder.append(operationType.getStringValue()).append('(')
                .append(columnNames.get(columnNames.size() - 1)).append(')')
                .append(" AS ").append(alias).append(' ')
                .append("FROM `properati-data-public.properties_ar.properties_rent_201501` ")
                .append("WHERE (surface_covered_in_m2 >= ")
                .append(bottom)
                .append(" AND surface_covered_in_m2 <= ")
                .append(top)
                .append(") ")
                .append("OR (surface_covered_in_m2 IS NULL) ")
                .append("GROUP BY ")
                .append(columnNames.get(0));
        for (String columnName: columnNames.subList(1, columnNames.size() - 1)) {
            queryStrBuilder.append(", ").append(columnName);
        }

        String queryStr = queryStrBuilder.toString();
        return QueryJobConfiguration
                    .newBuilder(queryStr)
                    .setUseLegacySql(false)
                    .build();
    }

    private TableResult getTableResultOfEstates(int bottom, int top) {
        BigQuery bigquery = getBigQueryInstance();
        if (bigquery == null) {
            return null;
        }

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

    /**
     * Creates tree based on given tableResult
     * Levels of tree are based on columnNames given in construction
     * Aggregation function is given in constructor (OperationType)
     *
     * @param tableResult query result received from bigQuery
     * @return root node of constructed tree
     */
    private Node createTree(TableResult tableResult) {
        if (tableResult == null) {
            return null;
        }
        NodeWithChildren root = new NodeWithChildren("");
//        List<String> columnNames = List.of("operation", "property_type", "country_name", "state_name",  "avg_price");

        tableResult.iterateAll().forEach(row -> createBranch(root, row));

        return root;
    }

    /**
     * Creates branch of tree starting from root == current.
     * If value of row on the same level is present then doesn't create new,
     *  if not present then creates new value on this level;
     * always create new value on the last level of tree.
     *
     * @param current starting node (== root)
     * @param row - row from TableResult with values for particular row in query result from database
     */
    private void createBranch(NodeWithChildren current, FieldValueList row) {
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
        columnValue = row.get(alias).getStringValue();
        current.addChild(new PriceNode(columnValue));
    }

    /**
     * Creates Object which can be serialized to json tree
     *
     * @param root element from which the tree starts
     * @return Object which serializes to json consisting tree when returned with REST
     */
    private Object constructJson(Node root) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            String json = mapper.writeValueAsString(root);

            return mapper.readValue(json, Object.class);
        } catch (JsonProcessingException ex) {
            ex.printStackTrace();
        }
        return null;
    }

    /**
     * Main method - gets tree with levels of columnNames with estates restricted by space by bottom and top
     *
     * @param bottom low border of space for estate
     * @param top high border of space for estate
     * @return Object which serializes to tree json
     */
    public Object getEstates(int bottom, int top) {
        TableResult result = getTableResultOfEstates(bottom, top);

        Node root = createTree(result);
        return constructJson(root);
    }
}
