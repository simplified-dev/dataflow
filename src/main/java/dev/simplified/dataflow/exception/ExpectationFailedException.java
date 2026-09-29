package dev.simplified.dataflow.exception;

import dev.simplified.dataflow.stage.transform.primitive.ExpectTransform;
import org.intellij.lang.annotations.PrintFormat;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Thrown when a value reaching an {@link ExpectTransform} fails the expectation it states.
 */
public class ExpectationFailedException extends RuntimeException {

    /**
     * Constructs a new {@code ExpectationFailedException} with the given cause.
     *
     * @param cause the underlying cause
     */
    public ExpectationFailedException(@NotNull Throwable cause) {
        super(cause);
    }

    /**
     * Constructs a new {@code ExpectationFailedException} with the given message.
     *
     * @param message the detail message
     */
    public ExpectationFailedException(@NotNull String message) {
        super(message);
    }

    /**
     * Constructs a new {@code ExpectationFailedException} with the given cause and message.
     *
     * @param cause the underlying cause
     * @param message the detail message
     */
    public ExpectationFailedException(@NotNull Throwable cause, @NotNull String message) {
        super(message, cause);
    }

    /**
     * Constructs a new {@code ExpectationFailedException} with the given formatted message.
     *
     * @param message the format string
     * @param args the format arguments
     */
    public ExpectationFailedException(@PrintFormat String message, @Nullable Object... args) {
        super(String.format(message, args));
    }

    /**
     * Constructs a new {@code ExpectationFailedException} with the given cause and formatted message.
     *
     * @param cause the underlying cause
     * @param message the format string
     * @param args the format arguments
     */
    public ExpectationFailedException(@NotNull Throwable cause, @PrintFormat String message, @Nullable Object... args) {
        super(String.format(message, args), cause);
    }

}
