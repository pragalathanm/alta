package org.pb.alta;

import static java.lang.Long.min;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import javax.enterprise.context.ApplicationScoped;
import lombok.extern.slf4j.Slf4j;
import org.pb.alta.exception.TableNotFoundException;

/**
 * This class is mainly used duplicating tables in case of ALTER commands.
 *
 * @author Pragalathan M <pragalathanm@gmail.com>
 */
@ApplicationScoped
@Slf4j
public class Table extends AbstractTable {

    public void duplicate(String table, Long from, Long till, final int chunkSize) throws SQLException {
        super.init(table);
        try {
            executeQuery("SHOW TABLES LIKE '_" + table + "'", String.class);
        } catch (Exception ex) {
            throw new TableNotFoundException("_" + table);
        }

        if (from == null) {
            from = getIdToCopyFrom();
        }
        if (till == null) {
            till = executeQuery("select MAX(" + primaryKey + ") from " + table, Long.class);
        }
        log.info("copying to temp table from {} to {}", from, till);
        log.info("Bulk insert statement: {}", bulkPstmt.toString());
        while (from != null && from < till) {
            duplicate(from, till, chunkSize, false);
            from = getIdToCopyFrom();
        }
    }

    public void duplicate(long from, long till, final int BATCH_SIZE, final boolean exceptionFlow) throws SQLException {
        int processed = 0;
        if (BATCH_SIZE == 1) {
            copyOneRow(from);
            return;
        }

        int size = BATCH_SIZE;
        int loopCount = 0;
        do {
            try {
                processed = copy(from, min(till, from + size));
                from += size;
                size = BATCH_SIZE;
            } catch (Exception ex) {
                //            } catch (CannotAcquireLockException | QueryTimeoutException ex) {
                log.error("Timeout {}", ex.getMessage());
                if (!exceptionFlow) {
                    from = getIdToCopyFrom();
                }
                if (size == 1) {
                    copyOneRow(from);
                    from += size;
                    processed = 1;
                } else {
                    duplicate(from, till, size / 2, true);
                    from = getIdToCopyFrom();
                    processed = -1;
                    loopCount = 0;
                }
                //            } catch (Exception ex) {
                //                log.error(null, ex);
            }
            loopCount++;
        } while (processed != 0 && (loopCount <= 1 || !exceptionFlow) && from < till);
        log.info("-----------" + size + "----------");
    }

    private void copyOneRow(long id) throws SQLException {
        LinkedHashMap<String, Object> result = executeQuery("SELECT * from " + table + " WHERE " + primaryKey + "=" + id);
        executeUpdate(singlePstmt, result);
    }

    private Long getIdToCopyFrom() throws SQLException {
        Long from = executeQuery("select MAX(" + primaryKey + ") from _" + table, Long.class);
        if (from != null) {
            from++;
            long old = from;
            from = executeQuery("select MIN(" + primaryKey + ") from " + table + " WHERE " + primaryKey + ">=" + from, Long.class);
            log.info("Using Id: {} from select MIN(" + primaryKey + ") from " + table + " WHERE " + primaryKey + ">={}", from, old);
        } else {
            from = executeQuery("select MIN(" + primaryKey + ") from " + table, Long.class);
            log.info("Using Id: {} from select MIN(" + primaryKey + ") from " + table, from);
        }
        return from;
    }

    private int copy(long from, long to) throws SQLException {
        log.info("Inserting {} >= {} < {} ", from, primaryKey, to);
        bulkPstmt.setQueryTimeout(15);
        bulkPstmt.setObject(1, from);
        bulkPstmt.setObject(2, to);
        int result = bulkPstmt.executeUpdate();
        log.info("Inserted: {}", result);
        return result;
    }
}
