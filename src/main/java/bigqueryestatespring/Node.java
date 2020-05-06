package bigqueryestatespring;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class Node {
    private final String data;
    private List<Node> children;

    public Node(String data) {
        this.data = data;
    }

    public String getData() {
        return data;
    }

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

    public boolean checkEqualData(String data) {
        return this.data.equals(data);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Node node = (Node) o;
        return data.equals(node.data);
    }

    @Override
    public int hashCode() {
        return Objects.hash(data);
    }
}
