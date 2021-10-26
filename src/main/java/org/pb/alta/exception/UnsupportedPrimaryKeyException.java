package org.pb.alta.exception;

/**
 *
 * @author Pragalathan M <pragalathanm@gmail.com>
 */
public class UnsupportedPrimaryKeyException extends RuntimeException {

    public UnsupportedPrimaryKeyException(String tableName) {
        super(tableName);
    }
}
