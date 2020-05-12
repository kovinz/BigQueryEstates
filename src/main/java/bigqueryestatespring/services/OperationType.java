package bigqueryestatespring.services;

public enum OperationType {
    AVERAGE("AVG"), MAXIMUM("MAX"), MINIMUM("MIN");

    private String stringValue;

    OperationType(String stringValue) {
        this.stringValue = stringValue;
    }

    public String getStringValue() {
        return stringValue;
    }
}
