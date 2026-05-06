package dev.sbs.dataflow;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

/**
 * Loads small text fixture bodies from {@code src/test/resources/fixtures/}.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class Fixtures {

    /**
     * Reads the named fixture as a UTF-8 string.
     *
     * @param name file name relative to {@code fixtures/}
     * @return the fixture body
     */
    public static @NotNull String load(@NotNull String name) {
        String path = "/fixtures/" + name;
        try (InputStream in = Fixtures.class.getResourceAsStream(path)) {
            if (in == null)
                throw new IllegalStateException("Fixture not found on classpath: " + path);
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to read fixture " + path, e);
        }
    }

}
