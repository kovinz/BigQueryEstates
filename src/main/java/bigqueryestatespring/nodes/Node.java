package bigqueryestatespring.nodes;

import java.util.Objects;

public abstract class Node {
    protected final String data;

    public Node(String data) {
        this.data = data;
    }

    //    @JsonGetter(value = "data")
    public String getData() {
        return data;
    }

    public boolean checkEqualData(String data) {
        return this.data.equals(data);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Node node = (NodeWithChildren) o;
        return data.equals(node.data);
    }

    @Override
    public int hashCode() {
        return Objects.hash(data);
    }
}
