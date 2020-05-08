package bigqueryestatespring.nodes;

import bigqueryestatespring.nodes.Node;

import java.util.ArrayList;
import java.util.List;

public class NodeWithChildren extends Node {
    private List<Node> children;

    public NodeWithChildren(String data) {
        super(data);
    }

//    @JsonGetter(value = "children")
    public List<Node> getChildren() {
        if (children == null) {
            children = new ArrayList<>();
        }
        return children;
    }

    public void addChild(Node newChild) {
        if (children == null) {
            children = new ArrayList<>();
        }
        children.add(newChild);
    }
}
