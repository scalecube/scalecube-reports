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
            }));
  }

  private static List<String[]> loadCSV(String string) {
    try (CSVReader csvReader = new CSVReader(new StringReader(string))) {
      return csvReader.readAll();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
