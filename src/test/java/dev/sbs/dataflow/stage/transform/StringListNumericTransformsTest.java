package dev.sbs.dataflow.stage.transform;

import dev.sbs.dataflow.stage.transform.dom.*;
import dev.sbs.dataflow.stage.transform.encoding.*;
import dev.sbs.dataflow.stage.transform.json.*;
import dev.sbs.dataflow.stage.transform.list.*;
import dev.sbs.dataflow.stage.transform.primitive.*;
import dev.sbs.dataflow.stage.transform.string.*;

import dev.sbs.dataflow.DataTypes;
import dev.sbs.dataflow.PipelineContext;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

class StringListNumericTransformsTest {

    private final PipelineContext ctx = PipelineContext.defaults();

    @Test
    @DisplayName("Lowercase / Uppercase / StringLength / Prefix / Suffix")
    void stringShape() {
        assertThat(LowerCaseTransform.of().execute(this.ctx, "AbC"), is(equalTo("abc")));
        assertThat(UpperCaseTransform.of().execute(this.ctx, "AbC"), is(equalTo("ABC")));
        assertThat(StringLengthTransform.of().execute(this.ctx, "hello"), is(equalTo(5)));
        assertThat(PrefixTransform.of(">>>").execute(this.ctx, "x"), is(equalTo(">>>x")));
        assertThat(SuffixTransform.of("<<<").execute(this.ctx, "x"), is(equalTo("x<<<")));
    }

    @Test
    @DisplayName("ListLength returns the input list size")
    void listLength() {
        Integer size = ListLengthTransform.of(DataTypes.STRING).execute(this.ctx, List.of("a", "b", "c"));
        assertThat(size, is(equalTo(3)));
    }

    @Test
    @DisplayName("Reverse returns the input list in reverse order")
    void reverse() {
        List<String> result = ReverseTransform.of(DataTypes.STRING).execute(this.ctx, List.of("a", "b", "c"));
        assertThat(result, contains("c", "b", "a"));
    }

    @Test
    @DisplayName("Abs(int/long/float/double) returns the magnitude")
    void abs() {
        assertThat(AbsIntTransform.of().execute(this.ctx, -5), is(equalTo(5)));
        assertThat(AbsLongTransform.of().execute(this.ctx, -5L), is(equalTo(5L)));
        assertThat(AbsFloatTransform.of().execute(this.ctx, -5.5f), is(equalTo(5.5f)));
        assertThat(AbsDoubleTransform.of().execute(this.ctx, -5.5), is(equalTo(5.5)));
    }

    @Test
    @DisplayName("Negate(int/long/float/double) flips the sign")
    void negate() {
        assertThat(NegateIntTransform.of().execute(this.ctx, 5), is(equalTo(-5)));
        assertThat(NegateLongTransform.of().execute(this.ctx, 5L), is(equalTo(-5L)));
        assertThat(NegateFloatTransform.of().execute(this.ctx, 5.5f), is(equalTo(-5.5f)));
        assertThat(NegateDoubleTransform.of().execute(this.ctx, 5.5), is(equalTo(-5.5)));
    }

    @Test
    @DisplayName("ToString invokes String.valueOf for the configured input type")
    void toStringTransform() {
        assertThat(ToStringTransform.of(DataTypes.INT).execute(this.ctx, 42), is(equalTo("42")));
        assertThat(ToStringTransform.of(DataTypes.DOUBLE).execute(this.ctx, 3.14), is(equalTo("3.14")));
        assertThat(ToStringTransform.of(DataTypes.BOOLEAN).execute(this.ctx, true), is(equalTo("true")));
    }

}
