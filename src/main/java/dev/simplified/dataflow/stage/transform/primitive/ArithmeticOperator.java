package dev.simplified.dataflow.stage.transform.primitive;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Arrays;

/**
 * Binary arithmetic operation applied by the arithmetic transforms, named on the wire by its
 * constant name.
 * <p>
 * Every operation reads as {@code left OP right}: the stage's input or left body is the left
 * operand, and its configured operand or right body is the right. The {@code apply} overloads
 * share one set of rules:
 * <ul>
 *   <li><b>{@code int} and {@code long}</b> - computed through the {@link Math} {@code *Exact}
 *       methods, so an overflow throws {@link ArithmeticException} rather than wrapping.
 *       {@link #DIVIDE} truncates toward zero.</li>
 *   <li><b>{@code float} and {@code double}</b> - computed in IEEE arithmetic of the operand
 *       type; a {@code NaN} or infinite result is answered with {@code null}.</li>
 *   <li><b>Zero divisor</b> - {@link #DIVIDE} and {@link #MODULO} answer {@code null} when the
 *       right operand is zero, for every type.</li>
 * </ul>
 */
public enum ArithmeticOperator {

    /**
     * Sum of the left and right operands.
     */
    ADD,

    /**
     * Left operand minus the right operand.
     */
    SUBTRACT,

    /**
     * Product of the left and right operands.
     */
    MULTIPLY,

    /**
     * Left operand divided by the right operand; an integral quotient truncates toward zero.
     */
    DIVIDE,

    /**
     * Floored remainder of the left operand divided by the right operand, as
     * {@link Math#floorMod(int, int)} computes it: the result is zero or has the sign of the right
     * operand, so a positive divisor never yields a negative remainder. Floating-point operands
     * follow the same rule.
     */
    MODULO;

    /**
     * Resolves an operator by its exact constant name.
     *
     * @param name the constant name, such as {@code MULTIPLY}
     * @return the operator
     * @throws IllegalArgumentException when no operator has that name
     */
    public static @NotNull ArithmeticOperator of(@NotNull String name) {
        for (ArithmeticOperator operator : values()) {
            if (operator.name().equals(name))
                return operator;
        }

        throw new IllegalArgumentException(
            "Unknown arithmetic operator '" + name + "', expected one of " + Arrays.toString(values())
        );
    }

    /**
     * Applies this operator to two {@code int} operands.
     *
     * @param left the left operand
     * @param right the right operand
     * @return the result, or {@code null} for a zero divisor
     * @throws ArithmeticException when the result overflows {@code int}
     */
    public @Nullable Integer apply(int left, int right) {
        if (right == 0 && this.divides()) return null;

        return switch (this) {
            case ADD -> Math.addExact(left, right);
            case SUBTRACT -> Math.subtractExact(left, right);
            case MULTIPLY -> Math.multiplyExact(left, right);
            case DIVIDE -> Math.divideExact(left, right);
            case MODULO -> Math.floorMod(left, right);
        };
    }

    /**
     * Applies this operator to two {@code long} operands.
     *
     * @param left the left operand
     * @param right the right operand
     * @return the result, or {@code null} for a zero divisor
     * @throws ArithmeticException when the result overflows {@code long}
     */
    public @Nullable Long apply(long left, long right) {
        if (right == 0 && this.divides()) return null;

        return switch (this) {
            case ADD -> Math.addExact(left, right);
            case SUBTRACT -> Math.subtractExact(left, right);
            case MULTIPLY -> Math.multiplyExact(left, right);
            case DIVIDE -> Math.divideExact(left, right);
            case MODULO -> Math.floorMod(left, right);
        };
    }

    /**
     * Applies this operator to two {@code float} operands in {@code float} arithmetic.
     *
     * @param left the left operand
     * @param right the right operand
     * @return the result, or {@code null} for a zero divisor or a {@code NaN} or infinite result
     */
    public @Nullable Float apply(float left, float right) {
        if (right == 0 && this.divides()) return null;

        float result = switch (this) {
            case ADD -> left + right;
            case SUBTRACT -> left - right;
            case MULTIPLY -> left * right;
            case DIVIDE -> left / right;
            case MODULO -> floorMod(left, right);
        };

        return Float.isFinite(result) ? result : null;
    }

    /**
     * Applies this operator to two {@code double} operands.
     *
     * @param left the left operand
     * @param right the right operand
     * @return the result, or {@code null} for a zero divisor or a {@code NaN} or infinite result
     */
    public @Nullable Double apply(double left, double right) {
        if (right == 0 && this.divides()) return null;

        double result = switch (this) {
            case ADD -> left + right;
            case SUBTRACT -> left - right;
            case MULTIPLY -> left * right;
            case DIVIDE -> left / right;
            case MODULO -> floorMod(left, right);
        };

        return Double.isFinite(result) ? result : null;
    }

    private boolean divides() {
        return this == DIVIDE || this == MODULO;
    }

    private static float floorMod(float left, float right) {
        float remainder = left % right;

        if (remainder == 0)
            return Math.copySign(0.0f, right);

        return (remainder < 0) == (right < 0) ? remainder : remainder + right;
    }

    private static double floorMod(double left, double right) {
        double remainder = left % right;

        if (remainder == 0)
            return Math.copySign(0.0, right);

        return (remainder < 0) == (right < 0) ? remainder : remainder + right;
    }

}
