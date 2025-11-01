package io.scalecube.reports.csv;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.opencsv.CSVReader;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public class CsvWriterTest {

  @ParameterizedTest
  @MethodSource("csvBlocksProvider")
  void testCsvGeneration(List<String[]> expected) {
    StringWriter out = new StringWriter();
    try (CsvWriter writer = new CsvWriter(out)) {
      for (String[] line : expected) {
        writer.writeNext(line);
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    List<String[]> loaded = loadCSV(out.toString());
    assertEquals(expected.size(), loaded.size());
    for (int i = 0; i < loaded.size(); i++) {
      assertArrayEquals(expected.get(i), loaded.get(i));
    }
  }

  static Stream<List<String[]>> csvBlocksProvider() {
    return Stream.of(
        List.of(
            new String[] {"hello", "world", "123"},
            new String[] {"with space", "leading ", " trailing"}),
        List.of(
            new String[] {"a,b,c", "simple", "field"},
            new String[] {"line\nbreak", "multi\nline\nfield", "ok"}),
        List.of(
            new String[] {"with space", "nothing", "13.455"},
            new String[] {"with\nnew line", " with a \"quote\"", "\""},
            new String[] {
              "with\nnew \"line\"", "random,separator,", "   with,\n\"all the,,,stuff \n\n  \""
            }),
        List.of(
            // simple fields
            new String[] {"hello", "world", "123"},
            // empty fields
            new String[] {"", null, "non-empty"},
            // fields with spaces
            new String[] {"with space", "leading ", " trailing"},
            // fields with quotes
            new String[] {"with\"quote", "\"start", "end\""},
            // fields with comma
            new String[] {"a,b,c", "simple", "field"},
            // fields with newlines
            new String[] {"line\nbreak", "multi\nline\nfield", "ok"},
            // fields with comma + quote + newline
            new String[] {"complex,\"\nfield", "another\nfield", "last"},
            // fields that are only quotes
            new String[] {"\"", "\"\"\"\"", "\""},
            // fields with Unicode characters
            new String[] {"😀", "こんにちは", "¡Hola!"},
            // mixed tricky
            new String[] {" \n \" , ", "normal", "end"}));
  }

  private static List<String[]> loadCSV(String string) {
    try (CSVReader csvReader = new CSVReader(new StringReader(string))) {
      return csvReader.readAll();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
