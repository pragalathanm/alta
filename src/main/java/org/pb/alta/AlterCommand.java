package org.pb.alta;

import io.quarkus.runtime.QuarkusApplication;
import io.quarkus.runtime.annotations.QuarkusMain;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import javax.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import picocli.CommandLine;

/**
 * Command that takes arguments from CLI and duplicates a table in MySQL.
 *
 * @author Pragalathan M <pragalathanm@gmail.com>
 */
@QuarkusMain
@CommandLine.Command
@Slf4j
public class AlterCommand implements Runnable, QuarkusApplication {

    @Inject
    CommandLine.IFactory factory;

    @CommandLine.Option(names = {"-t", "--table"}, description = "the name of the table")
    String tableName;

    @CommandLine.Option(names = {"-s", "--start-id"}, description = "ID of the record to copy from", required = false)
    String startId;

    @CommandLine.Option(names = {"-e", "--end-id"}, description = "ID of the recotd to copy till", required = false)
    String endId;

    @CommandLine.Option(names = {"-c", "--chunk-size"}, description = "the chunk of records that needs to be copied in one go", defaultValue = "1024", required = false)
    String chunkSize;

    private final Table table;
    private Long start;
    private Long end;
    private int chunk;

    public static final DateTimeFormatter ddMMyyyy = DateTimeFormatter.ofPattern("dd-MM-yyyy");

    public AlterCommand(Table table) {
        this.table = table;
    }

    @Override
    public int run(String... args) throws Exception {
        return new CommandLine(this, factory).execute(args);
    }

    @Override
    public void run() {
        validate();
        try {
            log.info("Preparing to duplicate {}", tableName);
            Instant d = Instant.now();
            table.duplicate(tableName, start, end, chunk);
            Duration time = Duration.between(d, Instant.now());
            log.info("Took: {}", humanReadableFormat(time));
        } catch (SQLException ex) {
            log.error("Error while duplicating the table {}", tableName, ex);
        }
    }

    private void validate() {
        if (tableName == null || tableName.isBlank()) {
            log.error("Invalid table name");
            CommandLine.usage(this, System.err);
            System.exit(0);
        }
        try {

            if (startId != null && !startId.isBlank()) {
                start = Long.parseLong(startId);
            }
            if (endId != null && !endId.isBlank()) {
                end = Long.parseLong(endId);
            }
            if (chunkSize != null && !chunkSize.isBlank()) {
                chunk = Integer.parseInt(chunkSize);
            }
        } catch (NumberFormatException ex) {
            log.error("Invalid start ID or end ID");
            CommandLine.usage(this, System.err);
            System.exit(0);
        }
    }

    public static String humanReadableFormat(Duration duration) {
        return duration.toString()
                .substring(2)
                .replaceAll("(\\d[HMS])(?!$)", "$1 ")
                .toLowerCase();
    }
}
