package org.pb.alta;

import io.agroal.api.AgroalDataSource;
import io.quarkus.runtime.ShutdownEvent;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map.Entry;
import javax.enterprise.event.Observes;
import javax.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.pb.alta.exception.UnsupportedPrimaryKeyException;

/**
 * @author Pragalathan M <pragalathanm@gmail.com>
 */
@Slf4j
public class AbstractTable {

    @Inject
    AgroalDataSource mysqlDS;

    private Connection mysqlConnection;
    private Statement statement;
    private LinkedHashSet<String> columns;
    private String columnsStr;

    protected String table;
    protected String primaryKey;
    protected PreparedStatement bulkPstmt;
    protected PreparedStatement singlePstmt;
    protected transient boolean go = true;

    void init(String table) throws SQLException {
        this.table = table;
        mysqlConnection = mysqlDS.getConnection();
        statement = mysqlConnection.createStatement();
        primaryKey = getPrimaryKey();
        columns = new LinkedHashSet<>(executeQuery("SELECT * from " + table + " LIMIT 1").keySet());
        columnsStr = String.join(",", columns);
        bulkPstmt = createBulkStatement();
        singlePstmt = createSingleRowStatement();
    }

    protected <T> T executeQuery(String query, Class<T> resultType) throws SQLException {
        try (ResultSet result = statement.executeQuery(query)) {
            if (result.next()) {
                return result.getObject(1, resultType);
            }
        }
        throw new RuntimeException("Error executing: " + query);
    }

    protected int executeUpdate(PreparedStatement pstmt, LinkedHashMap<String, Object> values) throws SQLException {
        int index = 1;
        for (Entry<String, Object> e : values.entrySet()) {
            Object value = e.getValue();
            pstmt.setObject(index++, value);
        }
        return pstmt.executeUpdate();

    }

    protected LinkedHashMap executeQuery(String query) throws SQLException {
        LinkedHashMap<String, Object> results = new LinkedHashMap<>();
        try (ResultSet result = statement.executeQuery(query)) {
            ResultSetMetaData metaData = result.getMetaData();
            int colCount = metaData.getColumnCount();
            if (result.next()) {
                for (int i = 1; i <= colCount; i++) {
                    results.put(metaData.getColumnLabel(i), result.getObject(i));
                }
                return results;
            }
        }
        throw new RuntimeException("Error executing: " + query);
    }

    private String getPrimaryKey() throws SQLException {
        try (ResultSet rs = mysqlConnection.getMetaData().getPrimaryKeys(null, mysqlConnection.getSchema(), table)) {
            while (rs.next()) {
                if (primaryKey != null) {
                    throw new UnsupportedPrimaryKeyException("Composite primay key is not supported");
                }
                return rs.getString("COLUMN_NAME");
            }
        }
        throw new UnsupportedPrimaryKeyException("No primary key found");
    }

    private PreparedStatement createBulkStatement() throws SQLException {
        String batchSql = "INSERT INTO _" + table
                + "(" + columnsStr + ")"
                + " SELECT  " + columnsStr
                + " FROM " + table
                + "\n WHERE " + primaryKey + " >= ? AND " + primaryKey + " < ?";
        return mysqlConnection.prepareStatement(batchSql);
    }

    private PreparedStatement createSingleRowStatement() throws SQLException {
        String singleRowSql = "INSERT INTO _" + table + " (" + columnsStr + ") values(" + String.join(",", Collections.nCopies(columns.size(), "?")) + ")";
        return mysqlConnection.prepareStatement(singleRowSql);
    }

    void onStop(@Observes ShutdownEvent ev) {
        go = false;
        log.info("The application is stopping...");
    }
}
