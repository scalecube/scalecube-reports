package io.scalecube.reports.csv;

import java.io.IOException;
import java.io.Writer;

public class CsvWriter implements AutoCloseable {

  private static final String separator = ",";
  private static final String quote = "\"";
  private static final String quoteEscape = quote;

  private final Writer writer;

  public CsvWriter(Writer writer) {
    this.writer = writer;
  }

  public void writeNext(String[] nextLine) {
    if (nextLine == null) {
      return;
    }
    for (int i = 0; i < nextLine.length; i++) {
      String field = nextLine[i];
      if (field == null) {
        field = "";
      }
      if (i > 0) {
        append(separator);
      }
      if (field.contains(quote) || field.contains(separator) || field.contains("\n")) {
        append(quote).append(field.replace(quote, quoteEscape + quote)).append(quote);
      } else {
        append(field);
      }
    }
    append("\n");
  }

  private CsvWriter append(String value) {
    try {
      writer.append(value);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return this;
  }

  @Override
  public void close() throws IOException {
    writer.close();
  }
}
