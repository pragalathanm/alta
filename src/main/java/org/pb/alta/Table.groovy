package org.pb.alta

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.pb.alta.exception.TableNotFoundException
import javax.enterprise.context.ApplicationScoped
import java.sql.SQLException

import static java.lang.Long.min

/**
 * This class is mainly used duplicating tables in case of ALTER commands.
 *
 * @author Pragalathan M <pragalathanm@gmail.com>
 */
@CompileStatic
@ApplicationScoped
@Slf4j
class Table extends AbstractTable {

    void duplicate(String table, Long from, Long till, final int chunkSize) throws SQLException {
        super.init(table);
        try {
            executeQuery("SHOW TABLES LIKE '_$table'", String.class);
        } catch (Exception ex) {
            throw new TableNotFoundException("_" + table);
        }

        if (from == null) {
            from = getIdToCopyFrom();
        }
        if (till == null) {
            till = executeQuery("select MAX($primaryKey) from $table", Long.class);
        }
        log.info("copying to temp table from {} to {}", from, till);
        log.info("Bulk insert statement: {}", bulkPstmt.toString());
        till++;
        while (go && from != null && from < till) {
            duplicate(from, till, chunkSize, false);
            if (!go) {
                break;
            }
            from = getIdToCopyFrom();
        }
        log.info("The application is stopped");
    }

    void duplicate(long from, long till, final int CHUNK_SIZE, final boolean exceptionFlow) throws SQLException {
        int processed;
        if (CHUNK_SIZE == 1) {
            copyOneRow(from);
            return;
        }

        int size = CHUNK_SIZE;
        int loopCount = 0;
        do {
            try {
                processed = copy(from, min(till, from + size));
                from += size;
                size = CHUNK_SIZE;
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
                    duplicate(from, till, (int) (size / 2), true);
                    from = getIdToCopyFrom();
                    processed = -1;
                    loopCount = 0;
                }
                //            } catch (Exception ex) {
                //                log.error(null, ex);
            }
            loopCount++;
        } while (go && processed != 0 && (loopCount <= 1 || !exceptionFlow) && from < till);
        log.info("-----------" + size + "----------");
    }

    private void copyOneRow(long id) throws SQLException {
        LinkedHashMap<String, Object> result = executeQuery("SELECT * from " + table + " WHERE " + primaryKey + "=" + id);
        executeUpdate(singlePstmt, result);
    }

    private Long getIdToCopyFrom() throws SQLException {
        Long from = executeQuery("select MAX($primaryKey) from _$table", Long.class);
        if (from != null) {
            from++;
            long old = from;
            from = executeQuery("select MIN($primaryKey) from $table WHERE $primaryKey >= $from", Long.class);
            log.info("Using Id: $from from select MIN($primaryKey) from $table WHERE $primaryKey >=$old");
        } else {
            from = executeQuery("select MIN($primaryKey) from $table", Long.class);
            log.info("Using Id: $from from select MIN($primaryKey) from $table");
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
