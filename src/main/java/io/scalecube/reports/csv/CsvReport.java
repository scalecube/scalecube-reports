package io.scalecube.reports.csv;

import static java.time.ZoneOffset.UTC;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public record CsvReport<T>(
    List<CsvColumn<T>> columns,
    DateTimeFormatter dateTimePattern,
    DateTimeFormatter datePattern,
    ZoneId zoneId,
    Map<Class<?>, Function<Object, String>> customFormatter) {

  private static final DateTimeFormatter DEFAULT_DATE_TIME_PATTERN =
      new DateTimeFormatterBuilder()
          .parseCaseInsensitive()
          .append(DateTimeFormatter.ISO_LOCAL_DATE)
          .appendLiteral(' ')
          .append(DateTimeFormatter.ISO_LOCAL_TIME)
          .toFormatter();

  private static final DateTimeFormatter DEFAULT_DATE_PATTERN =
      DateTimeFormatter.ofPattern("yyyy-MM-dd");

  public String[] columnsHeader() {
    return columns.stream().map(CsvColumn::columnName).toList().toArray(new String[0]);
  }

  public String[] mapRow(T row) {
    return columns.stream()
        .map(col -> toStringValue(col, col.mapper().apply(row)))
        .toList()
        .toArray(new String[0]);
  }

  public static class Builder<T> {

    private List<CsvColumn<T>> columns;
    private DateTimeFormatter dateTimePattern = DEFAULT_DATE_TIME_PATTERN;
    private DateTimeFormatter datePattern = DEFAULT_DATE_PATTERN;

    private ZoneId zoneId;
    private final Map<Class<?>, Function<Object, String>> customFormatter = new HashMap<>();

    public CsvReport<T> build() {
      return new CsvReport<>(columns, dateTimePattern, datePattern, zoneId, customFormatter);
    }

    public Builder<T> addColumn(String columnName, Function<T, Object> mapper) {
      return addColumn(columnName, null, mapper);
    }

    public Builder<T> addColumn(String columnName, ColumnType type, Function<T, Object> mapper) {
      if (columns == null) {
        columns = new ArrayList<>();
      }
      columns.add(new CsvColumn<>(columnName, type, mapper));
      return this;
    }

    public Builder<T> dateTimePattern(DateTimeFormatter dateTimePattern) {
      this.dateTimePattern = dateTimePattern;
      return this;
    }

    public Builder<T> datePattern(DateTimeFormatter datePattern) {
      this.datePattern = datePattern;
      return this;
    }

    public Builder<T> zoneId(ZoneId zoneId) {
      this.zoneId = zoneId;
      return this;
    }

    public Builder<T> customFormatter(Class<?> key, Function<Object, String> formatter) {
      customFormatter.put(key, formatter);
      return this;
    }
  }

  private String toStringValue(CsvColumn<T> column, Object value) {
    if (value == null) {
      return null;
    }

    Function<Object, String> formatter = customFormatter.get(value.getClass());
    if (formatter != null) {
      return formatter.apply(value);
    }

    if (value instanceof String) {
      return (String) value;
    }
    if (value instanceof BigDecimal) {
      return ((BigDecimal) value).stripTrailingZeros().toPlainString();
    }
    if (value instanceof LocalDateTime) {
      return applyTimeZone((LocalDateTime) value, zoneId).format(dateTimePattern);
    }
    if (value instanceof LocalDate) {
      return ((LocalDate) value).format(datePattern);
    }
    if (value instanceof Long) {
      if (column.type() == ColumnType.MILLISECONDS_DATE_TIME) {
        return applyTimeZoneMills((Long) value, zoneId).format(dateTimePattern);
      }
      if (column.type() == ColumnType.SECONDS_DATE_TIME) {
        return applyTimeZoneSeconds((Long) value, zoneId).format(dateTimePattern);
      }
    }

    return String.valueOf(value);
  }

  public static LocalDateTime applyTimeZone(LocalDateTime utcLocalDateTime, ZoneId timeZone) {
    if (timeZone == null) {
      return utcLocalDateTime;
    }

    return ZonedDateTime.of(utcLocalDateTime, UTC).withZoneSameInstant(timeZone).toLocalDateTime();
  }

  private static LocalDateTime applyTimeZoneSeconds(long utcEpochSeconds, ZoneId timeZone) {
    if (timeZone == null) {
      timeZone = ZoneOffset.UTC;
    }
    return Instant.ofEpochSecond(utcEpochSeconds).atZone(timeZone).toLocalDateTime();
  }

  private static LocalDateTime applyTimeZoneMills(long utcEpochMills, ZoneId timeZone) {
    if (timeZone == null) {
      timeZone = ZoneOffset.UTC;
    }
    return Instant.ofEpochMilli(utcEpochMills).atZone(timeZone).toLocalDateTime();
  }
}
