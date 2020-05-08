package bigqueryestatespring.exceptionMessages;

public enum ExceptionCode {
    CREDENTIALS_FILE_NOT_FOUND("Не удалось найти файл с credentials"),
    EXCEPTION_WHILE_READING_CREDENTIALS("Произошла ошибка во время чтения файла с credentials");

    private String message;

    ExceptionCode(String message) {
        this.message = message;
    }

    public String getMessage() {
        return message;
    }
}
