package dev.simplified.dataflow.stage.predicate.common;

import org.jetbrains.annotations.NotNull;

import java.util.Arrays;
import java.util.Comparator;

/**
 * Comparison applied by {@link ComparePredicate}, named on the wire by its constant name.
 * <p>
 * Every comparison reads as {@code left OP right}, and each constant decides from the sign of a
 * comparison result as a {@link Comparator} returns it: negative when the left value is less than
 * the right, zero when they are equal, positive when it is greater. {@link #EQUALS} and
 * {@link #NOT_EQUALS} test equality alone; the other four order the two values.
 */
public enum CompareOperator {

    /**
     * Left value equal to the right value.
     */
    EQUALS,

    /**
     * Left value not equal to the right value.
     */
    NOT_EQUALS,

    /**
     * Left value less than the right value.
     */
    LESS_THAN,

    /**
     * Left value less than or equal to the right value.
     */
    LESS_OR_EQUAL,

    /**
     * Left value greater than the right value.
     */
    GREATER_THAN,

    /**
     * Left value greater than or equal to the right value.
     */
    GREATER_OR_EQUAL;

    /**
     * Resolves an operator by its exact constant name.
     *
     * @param name the constant name, such as {@code LESS_THAN}
     * @return the operator
     * @throws IllegalArgumentException when no operator has that name
     */
    public static @NotNull CompareOperator of(@NotNull String name) {
        for (CompareOperator operator : values()) {
            if (operator.name().equals(name))
                return operator;
        }

        throw new IllegalArgumentException(
            "Unknown compare operator '" + name + "', expected one of " + Arrays.toString(values())
        );
    }

    /**
     * Whether this operator orders its operands rather than testing them for equality alone.
     *
     * @return {@code true} for {@link #LESS_THAN}, {@link #LESS_OR_EQUAL}, {@link #GREATER_THAN}
     *         and {@link #GREATER_OR_EQUAL}
     */
    public boolean isOrdering() {
        return this != EQUALS && this != NOT_EQUALS;
    }

    /**
     * Tests a comparison result of the left value against the right.
     *
     * @param comparison the comparison result - negative, zero or positive as the left value is
     *                   less than, equal to or greater than the right
     * @return whether the two values stand in this operator's relation
     */
    public boolean test(int comparison) {
        return switch (this) {
            case EQUALS -> comparison == 0;
            case NOT_EQUALS -> comparison != 0;
            case LESS_THAN -> comparison < 0;
            case LESS_OR_EQUAL -> comparison <= 0;
            case GREATER_THAN -> comparison > 0;
            case GREATER_OR_EQUAL -> comparison >= 0;
        };
    }

}
