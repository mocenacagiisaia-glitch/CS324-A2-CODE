package edu.usp.cs324.client;

import edu.usp.cs324.api.Job;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;

/** Headerless numeric CSV: comma separated fields, LF/CRLF records, optional quotes. */
public final class JobInput {
    private JobInput() { }

    public static Job read(Job.Type type, Path path) throws IOException {
        return parse(type, Files.readString(path, StandardCharsets.UTF_8));
    }

    public static Job parse(Job.Type type, String text) {
        if (text.startsWith("\uFEFF")) text = text.substring(1);
        List<Integer> numbers = new ArrayList<>();
        String[] rows = text.split("\\R", -1);
        for (int row = 0; row < rows.length; row++) {
            if (rows[row].isBlank()) continue;
            String[] fields = rows[row].split(",", -1);
            for (int col = 0; col < fields.length; col++) {
                String field = fields[col].strip();
                if (field.length() >= 2 && field.startsWith("\"") && field.endsWith("\""))
                    field = field.substring(1, field.length() - 1).strip();
                if (!field.matches("[+-]?[0-9]+"))
                    throw new IllegalArgumentException("Expected integer at row " + (row + 1) + ", column " + (col + 1));
                try { numbers.add(Integer.parseInt(field)); }
                catch (NumberFormatException e) {
                    throw new IllegalArgumentException("Integer out of range at row " + (row + 1) + ", column " + (col + 1), e);
                }
            }
        }
        if (type == Job.Type.PRIMESUM) {
            if (numbers.size() != 2) throw new IllegalArgumentException("PRIMESUM requires exactly start,end");
            return new Job(type, List.of(), numbers.get(0), numbers.get(1));
        }
        return new Job(type, numbers, 0, 0);
    }
}
