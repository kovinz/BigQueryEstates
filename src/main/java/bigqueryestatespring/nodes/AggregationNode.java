package bigqueryestatespring.nodes;

import com.fasterxml.jackson.annotation.JsonGetter;

import static bigqueryestatespring.services.PropertiesAttribute.PRICE;

public class AggregationNode extends Node {
    public AggregationNode(String data) {
        super(data);
    }

    /**
     * Made for purpose to rename data attribute in the last Node
     *
     * @return data
     */
    @JsonGetter(value = PRICE)
    public String getData() {
        return data;
    }
}
