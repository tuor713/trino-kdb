package org.uwh.trino.kdb;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class KDBFilter {
    private final KDBColumnHandle column;
    private final String pattern;

    @JsonCreator
    public KDBFilter(@JsonProperty("column") KDBColumnHandle column, @JsonProperty("pattern") String pattern) {
        this.column = column;
        this.pattern = pattern;
    }

    @JsonProperty("column")
    public KDBColumnHandle getColumn() {
        return column;
    }

    @JsonProperty("pattern")
    public String getPattern() {
        return pattern;
    }

    public String toKDBFilter() {
        return column.getName() + " like \"" + sqlToKdbPattern(pattern) + "\"";
    }

    private static String sqlToKdbPattern(String pattern) {
        return pattern
                .replaceAll("(\\[|\\])", "[$1]")
                .replaceAll("\\*", "[*]")
                .replaceAll("\\?", "[?]")
                .replaceAll("\"", "\\\\\"")
                .replaceAll("%", "*")
                .replaceAll("_", "?");
    }

    @Override
    public String toString() {
        return "KDBFilter{" +
                "column=" + column +
                ", pattern='" + pattern + '\'' +
                '}';
    }
}
