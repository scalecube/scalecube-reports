package io.scalecube.reports.csv;

import static java.time.format.DateTimeFormatter.ofPattern;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.opencsv.CSVReader;
import io.scalecube.reports.csv.CsvReport.Builder;
import java.io.File;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

public class CsvGeneratorTest {

  private static final String BASE_REPORT_NAME = "report";

  @ParameterizedTest
  @MethodSource("generateReportSuccessfullyMethodSource")
  void generateReportSuccessfully(TestData testData) {
    File reportFile =
        CsvGenerator.generateAsFile(testData.mapper, testData.dataSource, BASE_REPORT_NAME);
    assertReport(testData.columnNames, testData.expectedRows, reportFile);
  }

  private record TestData(
      Stream<Item> dataSource,
      Consumer<Builder<Item>> mapper,
      List<String> columnNames,
      List<String[]> expectedRows) {}

  @ParameterizedTest
  @MethodSource("generateReportFailureMethodSource")
  void generateReportFailure(TestDataFailure data) {
    StepVerifier.create(Mono.fromRunnable(data.executor))
        .expectErrorSatisfies(
            throwable ->
                assertThat(throwable)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage(data.errorMessage))
        .verify();
  }

  private static Stream<TestData> generateReportSuccessfullyMethodSource() {
    return Stream.of(
        new TestData(

            // Generates report using default formatters
            Stream.of(
                new Item(
                    1,
                    "record_1",
                    10.0D,
                    BigDecimal.valueOf(100, 2),
                    LocalDate.parse("2024-03-02"),
                    LocalDateTime.parse("2024-03-03T10:00:01"),
                    Status.ENABLED,
                    LocalDateTime.parse("2024-03-02T09:00:01").toEpochSecond(ZoneOffset.UTC)),
                new Item(
                    2,
                    "record_2",
                    11.1D,
                    BigDecimal.valueOf(200, 2),
                    LocalDate.parse("2024-03-04"),
                    LocalDateTime.parse("2024-03-05T10:00:02"),
                    Status.DISABLED,
                    LocalDateTime.parse("2024-03-04T09:00:01").toEpochSecond(ZoneOffset.UTC)),
                new Item(null, null, null, null, null, null, null, null)),
            builder ->
                builder
                    .addColumn("Item ID", Item::id)
                    .addColumn("Item name", Item::name)
                    .addColumn("Price", Item::price)
                    .addColumn("Quantity", Item::quantity)
                    .addColumn("Date", Item::date)
                    .addColumn("Timestamp", Item::timestamp)
                    .addColumn("Status", Item::status),
            List.of("Item ID", "Item name", "Price", "Quantity", "Date", "Timestamp", "Status"),
            List.of(
                new String[] {
                  "1", "record_1", "10.0", "1", "2024-03-02", "2024-03-03 10:00:01", "ENABLED"
                },
                new String[] {
                  "2", "record_2", "11.1", "2", "2024-03-04", "2024-03-05 10:00:02", "DISABLED"
                },
                new String[] {"", "", "", "", "", "", ""})),

        // Generates report setting formatters
        new TestData(
            Stream.of(
                new Item(
                    1,
                    "record_1",
                    10.0D,
                    BigDecimal.valueOf(100, 2),
                    LocalDate.parse("2024-03-02"),
                    LocalDateTime.parse("2024-03-03T10:00:01"),
                    Status.ENABLED,
                    LocalDateTime.parse("2024-03-02T09:00:01").toEpochSecond(ZoneOffset.UTC)),
                new Item(
                    2,
                    "record_2",
                    11.1D,
                    BigDecimal.valueOf(200, 2),
                    LocalDate.parse("2024-03-04"),
                    LocalDateTime.parse("2024-03-05T10:00:02"),
                    Status.DISABLED,
                    LocalDateTime.parse("2024-03-04T09:00:01").toEpochSecond(ZoneOffset.UTC)),
                new Item(null, null, null, null, null, null, null, null)),
            builder ->
                builder
                    .zoneId(ZoneId.of("Europe/Paris"))
                    .dateTimePattern(ofPattern("MM/dd/yyyy HH:mm:ss"))
                    .datePattern(ofPattern("MM/dd/yyyy"))
                    .addColumn("Item ID", Item::id)
                    .addColumn("Item name", Item::name)
                    .addColumn("Price", Item::price)
                    .addColumn("Quantity", Item::quantity)
                    .addColumn("Date", Item::date)
                    .addColumn("Timestamp", Item::timestamp)
                    .addColumn("Status", Item::status),
            List.of("Item ID", "Item name", "Price", "Quantity", "Date", "Timestamp", "Status"),
            List.of(
                new String[] {
                  "1", "record_1", "10.0", "1", "03/02/2024", "03/03/2024 11:00:01", "ENABLED"
                },
                new String[] {
                  "2", "record_2", "11.1", "2", "03/04/2024", "03/05/2024 11:00:02", "DISABLED"
                },
                new String[] {"", "", "", "", "", "", ""})),

        // Generates report setting custom formatters
        new TestData(
            Stream.of(
                new Item(
                    1,
                    "record_1",
                    10.0D,
                    BigDecimal.valueOf(100, 2),
                    LocalDate.parse("2024-03-02"),
                    LocalDateTime.parse("2024-03-03T10:00:01"),
                    Status.ENABLED,
                    LocalDateTime.parse("2024-03-02T09:00:01").toEpochSecond(ZoneOffset.UTC)),
                new Item(
                    2,
                    "record_2",
                    11.1D,
                    BigDecimal.valueOf(200, 2),
                    LocalDate.parse("2024-03-04"),
                    LocalDateTime.parse("2024-03-05T10:00:02"),
                    Status.DISABLED,
                    LocalDateTime.parse("2024-03-04T09:00:01").toEpochSecond(ZoneOffset.UTC)),
                new Item(null, null, null, null, null, null, null, null)),
            builder ->
                builder
                    .customFormatter(
                        LocalDate.class,
                        o -> ((LocalDate) o).format(DateTimeFormatter.ofPattern("yyyy MM dd")))
                    .customFormatter(Status.class, o -> ((Status) o).name().toLowerCase())
                    .customFormatter(BigDecimal.class, o -> ((BigDecimal) o).toPlainString())
                    .addColumn("Item ID", Item::id)
                    .addColumn("Quantity", Item::quantity)
                    .addColumn("Date", Item::date)
                    .addColumn("Timestamp", Item::timestamp)
                    .addColumn("Status", Item::status)
                    .addColumn("Created At", ColumnType.SECONDS_DATE_TIME, Item::createdAt),
            List.of("Item ID", "Quantity", "Date", "Timestamp", "Status", "Created At"),
            List.of(
                new String[] {
                  "1", "1.00", "2024 03 02", "2024-03-03 10:00:01", "enabled", "2024-03-02 09:00:01"
                },
                new String[] {
                  "2",
                  "2.00",
                  "2024 03 04",
                  "2024-03-05 10:00:02",
                  "disabled",
                  "2024-03-04 09:00:01"
                },
                new String[] {"", "", "", "", "", ""})),

        // Generates report with an empty data source stream
        new TestData(
            Stream.empty(),
            builder ->
                builder
                    .zoneId(ZoneId.of("Europe/Paris"))
                    .dateTimePattern(ofPattern("MM/dd/yyyy HH:mm:ss"))
                    .datePattern(ofPattern("MM/dd/yyyy"))
                    .addColumn("Item ID", Item::id)
                    .addColumn("Item name", Item::name)
                    .addColumn("Price", Item::price)
                    .addColumn("Quantity", Item::quantity)
                    .addColumn("Date", Item::date)
                    .addColumn("Timestamp", Item::timestamp)
                    .addColumn("Status", Item::status),
            List.of("Item ID", "Item name", "Price", "Quantity", "Date", "Timestamp", "Status"),
            List.of()),

        // Generates report with a null data source stream
        new TestData(
            null,
            builder ->
                builder
                    .zoneId(ZoneId.of("Europe/Paris"))
                    .dateTimePattern(ofPattern("MM/dd/yyyy HH:mm:ss"))
                    .datePattern(ofPattern("MM/dd/yyyy"))
                    .addColumn("Item ID", Item::id)
                    .addColumn("Item name", Item::name)
                    .addColumn("Price", Item::price)
                    .addColumn("Quantity", Item::quantity)
                    .addColumn("Date", Item::date)
                    .addColumn("Timestamp", Item::timestamp)
                    .addColumn("Status", Item::status),
            List.of("Item ID", "Item name", "Price", "Quantity", "Date", "Timestamp", "Status"),
            List.of()));
  }

  private static Stream<TestDataFailure> generateReportFailureMethodSource() {
    return Stream.of(
        new TestDataFailure(
            () -> CsvGenerator.generateAsFile(builder -> {}, Stream.empty(), BASE_REPORT_NAME),
            "Report definition columns could not be empty"),
        new TestDataFailure(
            () ->
                CsvGenerator.generateAsFile(
                    builder -> builder.addColumn(null, o -> ""), Stream.empty(), BASE_REPORT_NAME),
            "Report definition column[0] name is invalid"),
        new TestDataFailure(
            () ->
                CsvGenerator.generateAsFile(
                    builder -> builder.addColumn("", o -> "").build(),
                    Stream.empty(),
                    BASE_REPORT_NAME),
            "Report definition column[0] name is invalid"),
        new TestDataFailure(
            () ->
                CsvGenerator.generateAsFile(
                    builder -> builder.addColumn("COLUMN_1", null),
                    Stream.empty(),
                    BASE_REPORT_NAME),
            "Report definition column[0] mapper could not be null"),
        new TestDataFailure(
            () ->
                CsvGenerator.generateAsFile(
                    builder ->
                        builder.addColumn("COLUMN_1", item -> "").addColumn(null, item -> ""),
                    Stream.empty(),
                    BASE_REPORT_NAME),
            "Report definition column[1] name is invalid"));
  }

  private enum Status {
    ENABLED,
    DISABLED
  }

  private record TestDataFailure(Runnable executor, String errorMessage) {}

  private record Item(
      Integer id,
      String name,
      Double price,
      BigDecimal quantity,
      LocalDate date,
      LocalDateTime timestamp,
      Status status,
      Long createdAt) {}

  private void assertReport(
      List<String> expectedColumns, List<String[]> expectedRows, File reportFile) {

    assertThat(reportFile).isNotNull();
    List<String[]> reportData = loadReportData(reportFile.toPath());

    // Check column names
    String[] columnNames = reportData.get(0);
    assertEquals(expectedColumns.size(), columnNames.length, "numberOfColumns");
    for (int i = 0; i < expectedColumns.size(); i++) {
      String expectedColumn = expectedColumns.get(i);
      String columnName = columnNames[i];
      assertEquals(expectedColumn, columnName, expectedColumn);
    }

    // Check report data
    List<String[]> actualRows = reportData.subList(1, reportData.size());
    assertEquals(expectedRows.size(), actualRows.size(), "numberOfRows");
    for (int i = 0; i < expectedRows.size(); i++) {
      for (int j = 0; j < expectedColumns.size(); j++) {
        String expectedColumn = expectedColumns.get(j);
        assertEquals(expectedRows.get(i)[j], actualRows.get(i)[j], expectedColumn);
      }
    }
  }

  private static List<String[]> loadReportData(Path reportPath) {
    try (CSVReader csvReader = new CSVReader(Files.newBufferedReader(reportPath))) {
      return csvReader.readAll();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
