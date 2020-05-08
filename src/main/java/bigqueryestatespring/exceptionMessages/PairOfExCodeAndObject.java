package bigqueryestatespring.exceptionMessages;

public class PairOfExCodeAndObject {
    private ExceptionCode exceptionCode;
    private Object object;

    public PairOfExCodeAndObject(ExceptionCode exceptionCode) {
        this.exceptionCode = exceptionCode;
    }

    public PairOfExCodeAndObject(Object object) {
        this.object = object;
    }

    public ExceptionCode getExceptionCode() {
        return exceptionCode;
    }

    public Object getObject() {
        return object;
    }
}
