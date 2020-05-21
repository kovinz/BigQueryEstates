package bigqueryestatespring.services;

import bigqueryestatespring.nodes.AggregationNode;
import bigqueryestatespring.nodes.Node;
import bigqueryestatespring.nodes.NodeWithChildren;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.bigquery.*;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Record1;
import org.jooq.SelectSelectStep;
import org.jooq.conf.ParamType;
import org.jooq.impl.DSL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.stream.Collectors;

import static bigqueryestatespring.exceptionMessages.ExceptionMessage.*;
import static org.jooq.impl.DSL.*;


@Service
public class EstatesService implements DataService {
    private static final Logger logger = LoggerFactory.getLogger(EstatesService.class);
    private static final ExecutorService executorService = Executors
            .newFixedThreadPool(Runtime.getRuntime().availableProcessors());

    private static ObjectMapper mapper;
    private static BigQuery bigQuery;
    private static DSLContext dsl;

    @Autowired
    @Qualifier("defaultDslContextConfiguration")
    public void setDsl(DSLContext dslContext) {
        dsl = dslContext;
    }

    @Autowired
    @Qualifier("defaultObjectMapper")
    private void setMapper(ObjectMapper objectMapper) {
        mapper = objectMapper;
    }

    @Autowired
    private void setBigQuery(BigQuery bigQueryInstance) {
        bigQuery = bigQueryInstance;
    }

    public EstatesService() {}

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
    private QueryJobConfiguration getQueryJobConfiguration(List<String> columnNames, String aggregateColumn,
                                                           int bottom, int top) {
        Field<Double> price = field(aggregateColumn, Double.class);
        SelectSelectStep<Record1<BigDecimal>> query = dsl.select(avg(price).as("avg_" + aggregateColumn));
        if (columnNames != null) {
            List<Field<Object>> columnNamesFields = columnNames.stream().map(DSL::field).collect(Collectors.toList());
            query.select(columnNamesFields).groupBy(columnNamesFields);
        }
        query.from(table("`properati-data-public.properties_ar.properties_rent_201501`"))
                .where(field("surface_covered_in_m2").greaterOrEqual(bottom)
                        .and(field("surface_covered_in_m2").lessOrEqual(top)));
        return QueryJobConfiguration
                    .newBuilder(query.getSQL(ParamType.INLINED))
                    .setUseLegacySql(false)
                    .build();
    }

    private Future<TableResult> getTableResultOfEstates(List<String> columnNames, String aggregateColumn,
                                                        int bottom, int top) {
        QueryJobConfiguration queryConfig = getQueryJobConfiguration(columnNames, aggregateColumn, bottom, top);

        JobId jobId = JobId.of(UUID.randomUUID().toString());
        Job queryJob = bigQuery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());

        return executorService.submit(new RunJob(queryJob));
    }

    /**
     * Creates tree based on given tableResult
     * Levels of tree are based on columnNames given in construction
     * Aggregation function is given in constructor (OperationType)
     *
     * @param tableResult query result received from bigQuery
     * @return root node of constructed tree
     */
    private List<Node> createTree(List<String> columnNames, String aggregateColumn, TableResult tableResult) {
        List<Node> rootList = new ArrayList<>();

        tableResult.iterateAll().forEach(row -> createBranch(columnNames, aggregateColumn, rootList, row));

        return rootList;
    }

    /**
     * Creates branch of tree starting from root == current.
     * If value of row on the same level is present then doesn't create new,
     *  if not present then creates new value on this level;
     * always create new value on the last level of tree.
     *
     * @param currentList starting node (== root)
     * @param row - row from TableResult with values for particular row in query result from database
     */
    private void createBranch(List<String> columnNames, String aggregateColumn,
                              List<Node> currentList, FieldValueList row) {
        boolean exists;
        String columnValue;
        // Go through all column names except for the last one
        if (columnNames != null) {
            for (String columnName : columnNames) {
                exists = false;
                columnValue = row.get(columnName).getStringValue();
                for (Node node : currentList) {
                    // If node with the same data already exists - make it 'current'
                    if (node.checkEqualData(columnValue)) {
                        currentList = ((NodeWithChildren) node).getChildren();
                        exists = true;
                        break;
                    }
                }
                // If node with the same data doesn't exist - create it and then make it 'current'
                if (!exists) {
                    NodeWithChildren newNode = new NodeWithChildren(columnValue);
                    currentList.add(newNode);
                    currentList = newNode.getChildren();
                }
            }
        }
        // Last element is processed outside 'for' because we want to create different type of Node
        columnValue = row.get("avg_" + aggregateColumn).getStringValue();
        currentList.add(new AggregationNode(columnValue));
    }

    /**
     * Creates Object which can be serialized to json tree
     *
     * @param root element from which the tree starts
     * @return Object which serializes to json consisting tree when returned with REST
     */
    private Optional<JsonNode> constructJson(List<Node> root) {
        if (root == null || root.isEmpty()) {
            return Optional.empty();
        }
        try {
            String json = mapper.writeValueAsString(root);

            return Optional.of(mapper.readTree(json));
        } catch (JsonProcessingException ex) {
            throw new RuntimeException(EXCEPTION_WHILE_CREATE_JSON);
        }
    }

    /**
     * Main method - gets tree with levels of columnNames with estates restricted by space by bottom and top
     *
     * @param columnNames list of columns to create tree
     * @param aggregateColumn last element of the tree (will be processed in aggregate function)
     * @param bottom low border of space for estate
     * @param top high border of space for estate
     * @return Object which serializes to tree json
     */
    public Optional<JsonNode> getData(List<String> columnNames, String aggregateColumn, int bottom, int top) {
        if (bottom > top) {
            throw new RuntimeException(TOP_BORDER_UNDER_BOTTOM_BORDER);
        }
        if (aggregateColumn == null) {
            throw new RuntimeException(AGGREGATE_COLUMN_IS_NULL);
        }
        Future<TableResult> resultFuture = getTableResultOfEstates(columnNames, aggregateColumn, bottom, top);
        TableResult result;
        try {
            result = resultFuture.get();
        } catch (InterruptedException | ExecutionException ex) {
            logger.error(ERROR_WHILE_PROCESSING_QUERY);
            return Optional.empty();
        }
        if (result == null || result.getTotalRows() == 0) {
            return Optional.empty();
        }

        List<Node> root = createTree(columnNames, aggregateColumn, result);
        return constructJson(root);
    }

    static class RunJob implements Callable<TableResult> {
        Job queryJob;

        RunJob(Job queryJob) {
            this.queryJob = queryJob;
        }


        @Override
        public TableResult call() throws Exception {
            // Wait for the query to complete.
            queryJob = queryJob.waitFor();
            if (queryJob == null) {
                logger.error(JOB_NO_LONGER_EXISTS);
                return null;
            } else if (queryJob.getStatus().getError() != null) {
                logger.error(ERROR_WHILE_PROCESSING_QUERY);
                return null;
            }

            return queryJob.getQueryResults();
        }
    }
}
