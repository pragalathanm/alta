package org.pb.alta.exception;

/**
 *
 * @author Pragalathan M <pragalathanm@gmail.com>
 */
public class TableNotFoundException extends RuntimeException {

    public TableNotFoundException(String tableName) {
        super(tableName);
    }
}
