package bigqueryestatespring.services;

import bigqueryestatespring.exceptionMessages.ExceptionMessage;
import bigqueryestatespring.nodes.Node;
import bigqueryestatespring.nodes.NodeWithChildren;
import bigqueryestatespring.nodes.AggregationNode;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.*;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static bigqueryestatespring.exceptionMessages.ExceptionMessage.*;
import static bigqueryestatespring.services.PropertiesAttribute.*;

public class EstatesService implements Service {
    private static String PATH_TO_CREDENTIALS = System.getenv("PATH_TO_GCLOUD_CREDENTIALS");
    // list of columns to create tree
    private List<String> columnNames = Arrays.asList(OPERATION, PROPERTY_TYPE, COUNTRY_NAME, STATE_NAME);
    // aggregate function to process the last element
    private OperationType operationType = OperationType.AVERAGE;
    // last element of the tree (will be processed in aggregate function)
    private String aggregateColumn = PRICE;
    private String alias = operationType.getStringValue().toLowerCase() + '_' + aggregateColumn;

    public EstatesService() {}

    /**
     *
     * @param columnNames list of columns to create tree (last element will be processed in aggregate function)
     * @param operationType aggregate function to process the last element
     */
    public EstatesService(List<String> columnNames, OperationType operationType) {
        this.columnNames = columnNames;
        if (operationType != null) {
            this.operationType = operationType;
            this.alias = operationType.getStringValue().toLowerCase() + '_' + aggregateColumn;
        }
    }

    /**
     * Constructor with default operation type (AVG)
     *
     * @param columnNames list of columns to create tree (last element will be processed in aggregate function)
     */
    public EstatesService(List<String> columnNames) {
        this.columnNames = columnNames;
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
            throw new RuntimeException(CREDENTIALS_FILE_NOT_FOUND);
        } catch (IOException ex) {
            ex.printStackTrace();
            throw new RuntimeException(EXCEPTION_WHILE_READING_CREDENTIALS);
        }
    }

    /**
     * Gets QueryJobConfiguration which contains sql query as
     * SELECT columnNames(except last), operationType(columnNames(last))
     * FROM *** WHERE surface >= bottom and surface <= top
     * GROUP BY columnNames(except last)
     *
     * @param bottom low border for space
     * @param top high border for space
     * @return QueryJobConfiguration
     */
    private QueryJobConfiguration getQueryJobConfiguration(int bottom, int top) {
        StringBuilder queryStrBuilder = new StringBuilder("SELECT ");
        if (columnNames != null && !columnNames.isEmpty()) {
            for (String columnName : columnNames) {
                queryStrBuilder.append(columnName).append(", ");
            }
        }
        queryStrBuilder.append(operationType.getStringValue()).append('(')
                .append(aggregateColumn).append(')')
                .append(" AS ").append(alias).append(' ')
                .append("FROM `properati-data-public.properties_ar.properties_rent_201501` ")
                .append("WHERE (surface_covered_in_m2 >= ")
                .append(bottom)
                .append(" AND surface_covered_in_m2 <= ")
                .append(top)
                .append(") ");

        if (columnNames != null && !columnNames.isEmpty()) {
            queryStrBuilder.append("GROUP BY ").append(columnNames.get(0));
            for (String columnName : columnNames.subList(1, columnNames.size())) {
                queryStrBuilder.append(", ").append(columnName);
            }
        }

        return QueryJobConfiguration
                    .newBuilder(queryStrBuilder.toString())
                    .setUseLegacySql(false)
                    .build();
    }

    private TableResult getTableResultOfEstates(int bottom, int top) {
        BigQuery bigquery = getBigQueryInstance();

        QueryJobConfiguration queryConfig = getQueryJobConfiguration(bottom, top);

        JobId jobId = JobId.of(UUID.randomUUID().toString());
        Job queryJob = bigquery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());

        try {
            // Wait for the query to complete.
            queryJob = queryJob.waitFor();

            // Check for errors
            if (queryJob == null) {
                throw new RuntimeException(ExceptionMessage.JOB_NO_LONGER_EXISTS);
            } else if (queryJob.getStatus().getError() != null) {
                throw new RuntimeException(queryJob.getStatus().getError().toString());
            }

            return queryJob.getQueryResults();
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new RuntimeException(ex.getMessage());
        }
    }

    /**
     * Creates tree based on given tableResult
     * Levels of tree are based on columnNames given in construction
     * Aggregation function is given in constructor (OperationType)
     *
     * @param tableResult query result received from bigQuery
     * @return root node of constructed tree
     */
    private NodeWithChildren createTree(TableResult tableResult) {
        if (tableResult == null) {
            throw new RuntimeException(TABLE_RESULT_IS_NULL);
        }
        NodeWithChildren root = new NodeWithChildren("");

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
        if (columnNames != null) {
            for (String columnName : columnNames) {
                exists = false;
                columnValue = row.get(columnName).getStringValue();
                for (Node node : current.getChildren()) {
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
        }
        // Last element is processed outside 'for' because we want to create different type of Node
        columnValue = row.get(alias).getStringValue();
        current.addChild(new AggregationNode(columnValue));
    }

    /**
     * Creates Object which can be serialized to json tree
     *
     * @param root element from which the tree starts
     * @return Object which serializes to json consisting tree when returned with REST
     */
    private Optional<JsonNode> constructJson(NodeWithChildren root) {
        if (root == null || root.getChildren().isEmpty()) {
            return Optional.empty();
        }
        try {
            ObjectMapper mapper = new ObjectMapper();
            String json = mapper.writeValueAsString(root);

            return Optional.of(mapper.readTree(json));
        } catch (JsonProcessingException ex) {
            ex.printStackTrace();
            throw new RuntimeException(EXCEPTION_WHILE_CREATE_JSON);
        }
    }

    /**
     * Main method - gets tree with levels of columnNames with estates restricted by space by bottom and top
     *
     * @param bottom low border of space for estate
     * @param top high border of space for estate
     * @return Object which serializes to tree json
     */
    public Optional<JsonNode> getData(int bottom, int top) {
        if (bottom > top) {
            throw new RuntimeException(TOP_BORDER_UNDER_BOTTOM_BORDER);
        }
        TableResult result = getTableResultOfEstates(bottom, top);
        if (result.getTotalRows() == 0) {
            return Optional.empty();
        }

        NodeWithChildren root = createTree(result);
        return constructJson(root);
    }

    public String getAggregateColumn() {
        return aggregateColumn;
    }

    public void setAggregateColumn(String aggregateColumn) {
        this.aggregateColumn = aggregateColumn;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        // list of columns to create tree
        private List<String> columnNames = Arrays.asList(OPERATION, PROPERTY_TYPE, COUNTRY_NAME, STATE_NAME);
        // aggregate function to process the last element
        private OperationType operationType = OperationType.AVERAGE;
        // last element of the tree (will be processed in aggregate function)
        private String aggregateColumn = PRICE;

        private Builder() {
        }

        public static Builder anEstatesService() {
            return new Builder();
        }

        public Builder withColumnNames(List<String> columnNames) {
            this.columnNames = columnNames;
            return this;
        }

        public Builder withOperationType(OperationType operationType) {
            this.operationType = operationType;
            return this;
        }

        public Builder withAggregateColumn(String aggregateColumn) {
            this.aggregateColumn = aggregateColumn;
            return this;
        }

        public EstatesService build() {
            EstatesService estatesService = new EstatesService(columnNames, operationType);
            estatesService.setAggregateColumn(aggregateColumn);
            return estatesService;
        }
    }
}
