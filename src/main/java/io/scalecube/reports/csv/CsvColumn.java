package io.scalecube.reports.csv;

import java.util.function.Function;

public record CsvColumn<T>(String columnName, ColumnType type, Function<T, Object> mapper) {}
