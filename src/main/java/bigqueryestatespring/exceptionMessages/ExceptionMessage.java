package bigqueryestatespring.exceptionMessages;

public interface ExceptionMessage {
    String CREDENTIALS_FILE_NOT_FOUND = "Cannot find file with credentials";
    String EXCEPTION_WHILE_READING_CREDENTIALS = "Exception happened while reading file with credentials";
    String JOB_NO_LONGER_EXISTS = "Job no longer exists";
    String ERROR_WHILE_PROCESSING_QUERY = "Error occured while processing query";
}
