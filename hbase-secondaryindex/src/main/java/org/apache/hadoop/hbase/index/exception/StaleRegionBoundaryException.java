package org.apache.hadoop.hbase.index.exception;

import org.apache.hadoop.hbase.DoNotRetryIOException;

public class StaleRegionBoundaryException extends DoNotRetryIOException {
  // TODO: This would be more useful as a marker interface than as a class.
  private static final long serialVersionUID = 1197446454511704140L;

  /**
   * default constructor
   */
  public StaleRegionBoundaryException() {
    super();
  }

  /**
   * @param message
   */
  public StaleRegionBoundaryException(String message) {
    super(message);
  }

  /**
   * @param message
   * @param cause
   */
  public StaleRegionBoundaryException(String message, Throwable cause) {
    super(message, cause);
  }

  public StaleRegionBoundaryException(Throwable cause) {
    super(cause);
  }
}
