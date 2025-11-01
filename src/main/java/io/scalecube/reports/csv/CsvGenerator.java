package io.scalecube.reports.csv;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.function.Consumer;
import java.util.stream.Stream;

public class CsvGenerator {

  private static final String TEMP_DIR = System.getProperty("java.io.tmpdir");
  private static final DateTimeFormatter FORMATTER =
      DateTimeFormatter.ofPattern("yyyy-MM-dd_HH-mm-ss-SSS");

  private CsvGenerator() {
    // Do not instantiate
  }

  public static <T> File generateAsFile(
      Consumer<CsvReport.Builder<T>> builder, Stream<T> dataSource, String baseName) {
    final CsvReport.Builder<T> reportBuilder = new CsvReport.Builder<>();
    builder.accept(reportBuilder);
    final var reportDefinition = reportBuilder.build();

    validateReport(reportDefinition);

    try {
      File reportFile = createFile(baseName + "-", ".csv");

      try (var writer = new FileWriter(reportFile);
          var csvWriter = new CsvWriter(writer);
          Stream<T> rows = dataSource) {

        csvWriter.writeNext(reportDefinition.columnsHeader());
        if (rows != null) {
          rows.map(reportDefinition::mapRow).forEach(csvWriter::writeNext);
        }

        return reportFile;
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static File createFile(String prefix, String suffix) throws IOException {
    final var tempFile = new File(new File(TEMP_DIR), generateFileName(prefix, suffix));

    boolean isOutsideTempDir = !tempFile.toPath().normalize().startsWith(Paths.get(TEMP_DIR));
    if (isOutsideTempDir || !tempFile.createNewFile()) {
      throw new IOException("Could not create temp file: " + tempFile.getAbsolutePath());
    }

    return tempFile;
  }

  private static String generateFileName(String prefix, String suffix) {
    return prefix + LocalDateTime.now().format(FORMATTER) + suffix;
  }

  private static void validateReport(CsvReport<?> csvReport) {
    if (csvReport == null) {
      throw new IllegalArgumentException("Report definition could not be null");
    }
    if (csvReport.columns() == null || csvReport.columns().isEmpty()) {
      throw new IllegalArgumentException("Report definition columns could not be empty");
    }
    for (int i = 0; i < csvReport.columns().size(); i++) {
      CsvColumn<?> csvColumn = csvReport.columns().get(i);
      if (csvColumn.columnName() == null || csvColumn.columnName().isBlank()) {
        throw new IllegalArgumentException("Report definition column[" + i + "] name is invalid");
      }
      if (csvColumn.mapper() == null) {
        throw new IllegalArgumentException(
            "Report definition column[" + i + "] mapper could not be null");
      }
    }
  }
}
