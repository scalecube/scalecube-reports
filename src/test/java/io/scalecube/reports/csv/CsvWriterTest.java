package io.scalecube.reports.csv;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.opencsv.CSVReader;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.List;
import org.junit.jupiter.api.Test;

public class CsvWriterTest {

  @Test
  void testCsvGeneration() {
    StringWriter out = new StringWriter();
    List<String[]> expected =
        List.of(
            new String[] {"with space", "nothing", "13.455"},
            new String[] {"with\nnew line", " with a \"quote\"", "\""},
            new String[] {
              "with\nnew \"line\"", "random,separator,", "   with,\n\"all the,,,stuff \n\n  \""
            });
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

  private static List<String[]> loadCSV(String string) {
    try (CSVReader csvReader = new CSVReader(new StringReader(string))) {
      return csvReader.readAll();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
