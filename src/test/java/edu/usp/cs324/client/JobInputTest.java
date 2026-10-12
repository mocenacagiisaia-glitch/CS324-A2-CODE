package edu.usp.cs324.client;

import edu.usp.cs324.api.*;
import java.nio.file.*;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

class JobInputTest {
    @TempDir Path directory;
    @Test void csvAndManualHaveSameSemantics() throws Exception {
        String csv = "\uFEFF\"-2\", +3\r\n\n5,5\r\n";
        Path path = directory.resolve("numbers.csv");
        Files.writeString(path, csv);
        assertEquals(List.of(-2, 3, 5, 5), JobInput.read(Job.Type.MAX, path).numbers());
        assertEquals(JobInput.parse(Job.Type.MAX, csv), JobInput.read(Job.Type.MAX, path));
        assertEquals(-2, JobInput.parse(Job.Type.PRIMESUM, "-2\n10").start());
    }
    @Test void malformedInputsAreRejected() {
        for (String text : List.of("1,", ",1", "1,,2", "number", "1.5", "2147483648", "\"1", "1 2", "\"1,2\""))
            assertThrows(IllegalArgumentException.class, () -> JobInput.parse(Job.Type.MAX, text), text);
        assertThrows(IllegalArgumentException.class, () -> JobInput.parse(Job.Type.MAX, ""));
        for (String text : List.of("1", "1,2,3", "3,1"))
            assertThrows(IllegalArgumentException.class, () -> JobInput.parse(Job.Type.PRIMESUM, text));
        assertTrue(JobInput.parse(Job.Type.PRIMECOUNT, "").numbers().isEmpty());
    }
}
