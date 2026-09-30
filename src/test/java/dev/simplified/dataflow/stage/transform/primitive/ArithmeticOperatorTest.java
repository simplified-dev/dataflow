package dev.simplified.dataflow.stage.transform.primitive;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ArithmeticOperatorTest {

    @Test
    @DisplayName("of resolves an operator by its constant name")
    void ofResolvesName() {
        assertThat(ArithmeticOperator.of("MODULO"), is(ArithmeticOperator.MODULO));
    }

    @Test
    @DisplayName("of refuses a name that differs only in case")
    void ofIsCaseSensitive() {
        assertThrows(IllegalArgumentException.class, () -> ArithmeticOperator.of("add"));
    }

    @Test
    @DisplayName("of names the unknown operator in its message")
    void ofNamesUnknownOperator() {
        IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class, () -> ArithmeticOperator.of("POWER"));
        assertThat(thrown.getMessage(), startsWith("Unknown arithmetic operator 'POWER'"));
    }

    @Test
    @DisplayName("int ADD sums the operands")
    void intAdd() {
        assertThat(ArithmeticOperator.ADD.apply(7, 5), is(equalTo(12)));
    }

    @Test
    @DisplayName("int SUBTRACT takes the right operand from the left")
    void intSubtract() {
        assertThat(ArithmeticOperator.SUBTRACT.apply(7, 5), is(equalTo(2)));
    }

    @Test
    @DisplayName("int MULTIPLY multiplies the operands")
    void intMultiply() {
        assertThat(ArithmeticOperator.MULTIPLY.apply(7, 5), is(equalTo(35)));
    }

    @Test
    @DisplayName("int DIVIDE divides the left operand by the right")
    void intDivide() {
        assertThat(ArithmeticOperator.DIVIDE.apply(35, 5), is(equalTo(7)));
    }

    @Test
    @DisplayName("int DIVIDE truncates a positive quotient toward zero")
    void intDivideTruncatesPositive() {
        assertThat(ArithmeticOperator.DIVIDE.apply(7, 2), is(equalTo(3)));
    }

    @Test
    @DisplayName("int DIVIDE truncates a negative quotient toward zero")
    void intDivideTruncatesNegative() {
        assertThat(ArithmeticOperator.DIVIDE.apply(-7, 2), is(equalTo(-3)));
    }

    @Test
    @DisplayName("int MODULO of a negative dividend by a positive divisor is not negative")
    void intModuloFloorsNegativeDividend() {
        assertThat(ArithmeticOperator.MODULO.apply(-1, 5), is(equalTo(4)));
    }

    @Test
    @DisplayName("int MODULO takes the sign of a negative divisor")
    void intModuloFollowsDivisorSign() {
        assertThat(ArithmeticOperator.MODULO.apply(1, -5), is(equalTo(-4)));
    }

    @Test
    @DisplayName("int DIVIDE by zero rejects with null")
    void intDivideByZero() {
        assertThat(ArithmeticOperator.DIVIDE.apply(7, 0), is(nullValue()));
    }

    @Test
    @DisplayName("int MODULO by zero rejects with null")
    void intModuloByZero() {
        assertThat(ArithmeticOperator.MODULO.apply(7, 0), is(nullValue()));
    }

    @Test
    @DisplayName("int ADD with a zero right operand is not a zero divisor")
    void intAddZero() {
        assertThat(ArithmeticOperator.ADD.apply(7, 0), is(equalTo(7)));
    }

    @Test
    @DisplayName("int ADD overflow throws rather than wrapping")
    void intAddOverflowThrows() {
        assertThrows(ArithmeticException.class, () -> ArithmeticOperator.ADD.apply(Integer.MAX_VALUE, 1));
    }

    @Test
    @DisplayName("int SUBTRACT overflow throws rather than wrapping")
    void intSubtractOverflowThrows() {
        assertThrows(ArithmeticException.class, () -> ArithmeticOperator.SUBTRACT.apply(Integer.MIN_VALUE, 1));
    }

    @Test
    @DisplayName("int MULTIPLY overflow throws rather than wrapping")
    void intMultiplyOverflowThrows() {
        assertThrows(ArithmeticException.class, () -> ArithmeticOperator.MULTIPLY.apply(Integer.MAX_VALUE, 2));
    }

    @Test
    @DisplayName("int DIVIDE of MIN_VALUE by -1 throws rather than wrapping")
    void intDivideOverflowThrows() {
        assertThrows(ArithmeticException.class, () -> ArithmeticOperator.DIVIDE.apply(Integer.MIN_VALUE, -1));
    }

    @Test
    @DisplayName("long MULTIPLY computes past the int range")
    void longMultiply() {
        assertThat(ArithmeticOperator.MULTIPLY.apply(3_000_000_000L, 2L), is(equalTo(6_000_000_000L)));
    }

    @Test
    @DisplayName("long DIVIDE truncates a negative quotient toward zero")
    void longDivideTruncatesNegative() {
        assertThat(ArithmeticOperator.DIVIDE.apply(-7L, 2L), is(equalTo(-3L)));
    }

    @Test
    @DisplayName("long MODULO of a negative dividend by a positive divisor is not negative")
    void longModuloFloors() {
        assertThat(ArithmeticOperator.MODULO.apply(-1L, 5L), is(equalTo(4L)));
    }

    @Test
    @DisplayName("long DIVIDE by zero rejects with null")
    void longDivideByZero() {
        assertThat(ArithmeticOperator.DIVIDE.apply(7L, 0L), is(nullValue()));
    }

    @Test
    @DisplayName("long ADD overflow throws rather than wrapping")
    void longAddOverflowThrows() {
        assertThrows(ArithmeticException.class, () -> ArithmeticOperator.ADD.apply(Long.MAX_VALUE, 1L));
    }

    @Test
    @DisplayName("long DIVIDE of MIN_VALUE by -1 throws rather than wrapping")
    void longDivideOverflowThrows() {
        assertThrows(ArithmeticException.class, () -> ArithmeticOperator.DIVIDE.apply(Long.MIN_VALUE, -1L));
    }

    @Test
    @DisplayName("float DIVIDE keeps the fraction")
    void floatDivide() {
        assertThat(ArithmeticOperator.DIVIDE.apply(7f, 2f), is(equalTo(3.5f)));
    }

    @Test
    @DisplayName("float MODULO of a negative dividend by a positive divisor is not negative")
    void floatModuloFloors() {
        assertThat(ArithmeticOperator.MODULO.apply(-1.5f, 4f), is(equalTo(2.5f)));
    }

    @Test
    @DisplayName("float MODULO of a remainder that would round onto a positive divisor is positive zero")
    void floatModuloRoundingOntoDivisorIsZero() {
        assertThat(ArithmeticOperator.MODULO.apply(-1e-7f, 360f), is(equalTo(0.0f)));
    }

    @Test
    @DisplayName("float DIVIDE by zero rejects with null")
    void floatDivideByZero() {
        assertThat(ArithmeticOperator.DIVIDE.apply(7f, 0f), is(nullValue()));
    }

    @Test
    @DisplayName("float DIVIDE by negative zero rejects with null")
    void floatDivideByNegativeZero() {
        assertThat(ArithmeticOperator.DIVIDE.apply(7f, -0f), is(nullValue()));
    }

    @Test
    @DisplayName("float MULTIPLY overflowing to infinity rejects with null")
    void floatOverflowRejects() {
        assertThat(ArithmeticOperator.MULTIPLY.apply(Float.MAX_VALUE, 2f), is(nullValue()));
    }

    @Test
    @DisplayName("float ADD of a NaN operand rejects with null")
    void floatNaNRejects() {
        assertThat(ArithmeticOperator.ADD.apply(Float.NaN, 1f), is(nullValue()));
    }

    @Test
    @DisplayName("double SUBTRACT takes the right operand from the left")
    void doubleSubtract() {
        assertThat(ArithmeticOperator.SUBTRACT.apply(1.5, 4.0), is(equalTo(-2.5)));
    }

    @Test
    @DisplayName("double MODULO of a negative dividend by a positive divisor is not negative")
    void doubleModuloFloors() {
        assertThat(ArithmeticOperator.MODULO.apply(-1.5, 4.0), is(equalTo(2.5)));
    }

    @Test
    @DisplayName("double MODULO takes the sign of a negative divisor")
    void doubleModuloFollowsDivisorSign() {
        assertThat(ArithmeticOperator.MODULO.apply(1.5, -4.0), is(equalTo(-2.5)));
    }

    @Test
    @DisplayName("double MODULO with no remainder is a zero carrying the divisor's sign")
    void doubleModuloZeroCarriesDivisorSign() {
        assertThat(ArithmeticOperator.MODULO.apply(8.0, -4.0), is(equalTo(-0.0)));
    }

    @Test
    @DisplayName("double MODULO of a remainder that would round onto a positive divisor is positive zero")
    void doubleModuloRoundingOntoDivisorIsZero() {
        assertThat(ArithmeticOperator.MODULO.apply(-1e-14, 360.0), is(equalTo(0.0)));
    }

    @Test
    @DisplayName("double MODULO of a remainder that would round onto a negative divisor is negative zero")
    void doubleModuloRoundingOntoNegativeDivisorIsNegativeZero() {
        assertThat(ArithmeticOperator.MODULO.apply(1e-14, -360.0), is(equalTo(-0.0)));
    }

    @Test
    @DisplayName("double MODULO by zero rejects with null")
    void doubleModuloByZero() {
        assertThat(ArithmeticOperator.MODULO.apply(7.0, 0.0), is(nullValue()));
    }

    @Test
    @DisplayName("double MULTIPLY overflowing to infinity rejects with null")
    void doubleOverflowRejects() {
        assertThat(ArithmeticOperator.MULTIPLY.apply(Double.MAX_VALUE, 2.0), is(nullValue()));
    }

    @Test
    @DisplayName("double ADD of an infinite operand rejects with null")
    void doubleInfiniteRejects() {
        assertThat(ArithmeticOperator.ADD.apply(Double.POSITIVE_INFINITY, 1.0), is(nullValue()));
    }

    @Test
    @DisplayName("double MODULO of an infinite dividend rejects with null")
    void doubleModuloInfiniteDividendRejects() {
        assertThat(ArithmeticOperator.MODULO.apply(Double.POSITIVE_INFINITY, 3.0), is(nullValue()));
    }

}
