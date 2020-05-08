package bigqueryestatespring.nodes;

import bigqueryestatespring.nodes.Node;
import com.fasterxml.jackson.annotation.JsonGetter;

public class PriceNode extends Node {
    public PriceNode(String data) {
        super(data);
    }

    @JsonGetter(value = "price")
    public String getData() {
        return data;
    }
}
