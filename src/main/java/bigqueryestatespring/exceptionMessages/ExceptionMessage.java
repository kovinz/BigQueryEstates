package bigqueryestatespring.exceptionMessages;

public interface ExceptionMessage {
    String CREDENTIALS_FILE_NOT_FOUND = "Cannot find file with credentials";
    String EXCEPTION_WHILE_READING_CREDENTIALS = "Exception happened while reading file with credentials";
    String JOB_NO_LONGER_EXISTS = "Job no longer exists";
    String ERROR_WHILE_PROCESSING_QUERY = "Error occurred while processing query";
    String NOT_GIVEN_OPERATION_TYPE = "Operation type is null";
    String TOP_BORDER_UNDER_BOTTOM_BORDER = "Top argument is less than bottom argument";
    String TABLE_RESULT_IS_NULL = "Table result is null";
    String EXCEPTION_WHILE_CREATE_JSON = "Exception happened while creating json";
    String ARGUMENTS_ARE_NEGATIVE = "One or two arguments are less then zero";
    String AGGREGATE_COLUMN_IS_NULL = "Column to be processed in the aggregation function is not specified";
    String PATH_TO_GCLOUD_CREDENTIALS_IS_NOT_SPECIFIED = "Environment variable PATH_TO_GCLOUD_CREDENTIALS doesn't exist or empty";
}
