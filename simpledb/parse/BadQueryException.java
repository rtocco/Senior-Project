package simpledb.parse;

/**
 * A runtime exception indicating that the submitted query
 * has incompatible field values
 */
@SuppressWarnings("serial")
public class BadQueryException extends RuntimeException {
   public BadQueryException() {
   }
}
