package bigqueryestatespring.services;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.List;
import java.util.Optional;

public interface DataService {
    Optional<JsonNode> getData(List<String> columnNames, String aggregateColumn, int bottom, int top);
}
