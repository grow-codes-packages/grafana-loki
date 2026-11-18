package codes.grow.packages.loki;

/** Result type of a Loki query. */
public enum ResultType {
  /** Log streams ({@link QueryResult#streams()}). */
  STREAMS,
  /** One sample per series ({@link QueryResult#vector()}). */
  VECTOR,
  /** Sample series ({@link QueryResult#matrix()}). */
  MATRIX,
  /** A single value ({@link QueryResult#scalar()}). */
  SCALAR
}
