package bigqueryestatespring.services;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.Optional;

public interface Service {
    Optional<JsonNode> getData(int bottom, int top);
}
