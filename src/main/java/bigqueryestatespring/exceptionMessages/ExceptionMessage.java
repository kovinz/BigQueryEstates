package bigqueryestatespring.exceptionMessages;

public interface ExceptionMessage {
    String CREDENTIALS_FILE_NOT_FOUND = "Cannot find file with credentials";
    String EXCEPTION_WHILE_READING_CREDENTIALS = "Exception happened while reading file with credentials";
    String JOB_NO_LONGER_EXISTS = "Job no longer exists";
    String ERROR_WHILE_PROCESSING_QUERY = "Error occured while processing query";
    String NOT_GIVEN_OPERATION_TYPE = "Operation type is null";
    String NOT_GIVEN_COLUMN_NAMES = "Column names is null or length < 2";
    String TOP_BORDER_UNDER_BOTTOM_BORDER = "Top argument is less than bottom argument";
    String TABLE_RESULT_IS_NULL = "Table result is null";
    String EXCEPTION_WHILE_CREATE_JSON = "Exception happened while creating json";
}
