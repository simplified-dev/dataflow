package dev.simplified.dataflow.stage.transform;

import dev.simplified.dataflow.DataTypes;
import dev.simplified.dataflow.PipelineContext;
import dev.simplified.dataflow.stage.transform.list.SizeTransform;
import dev.simplified.dataflow.stage.transform.list.ReverseTransform;
import dev.simplified.dataflow.stage.transform.primitive.AbsDoubleTransform;
import dev.simplified.dataflow.stage.transform.primitive.AbsFloatTransform;
import dev.simplified.dataflow.stage.transform.primitive.AbsIntTransform;
import dev.simplified.dataflow.stage.transform.primitive.AbsLongTransform;
import dev.simplified.dataflow.stage.transform.primitive.NegateDoubleTransform;
import dev.simplified.dataflow.stage.transform.primitive.NegateFloatTransform;
import dev.simplified.dataflow.stage.transform.primitive.NegateIntTransform;
import dev.simplified.dataflow.stage.transform.primitive.NegateLongTransform;
import dev.simplified.dataflow.stage.transform.primitive.ToStringTransform;
import dev.simplified.dataflow.stage.transform.string.AppendTransform;
import dev.simplified.dataflow.stage.transform.string.CamelCaseTransform;
import dev.simplified.dataflow.stage.transform.string.LengthTransform;
import dev.simplified.dataflow.stage.transform.string.LowerCaseTransform;
import dev.simplified.dataflow.stage.transform.string.PascalCaseTransform;
import dev.simplified.dataflow.stage.transform.string.PrependTransform;
import dev.simplified.dataflow.stage.transform.string.SnakeCaseTransform;
import dev.simplified.dataflow.stage.transform.string.TitleCaseTransform;
import dev.simplified.dataflow.stage.transform.string.UpperCaseTransform;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

class StringListNumericTransformsTest {

    private final PipelineContext ctx = PipelineContext.defaults();

    @Test
    @DisplayName("Lowercase / Uppercase / StringLength / Prefix / Suffix")
    void stringShape() {
        assertThat(LowerCaseTransform.of().execute(this.ctx, "AbC"), is(equalTo("abc")));
        assertThat(UpperCaseTransform.of().execute(this.ctx, "AbC"), is(equalTo("ABC")));
        assertThat(LengthTransform.of().execute(this.ctx, "hello"), is(equalTo(5)));
        assertThat(PrependTransform.of(">>>").execute(this.ctx, "x"), is(equalTo(">>>x")));
        assertThat(AppendTransform.of("<<<").execute(this.ctx, "x"), is(equalTo("x<<<")));
    }

    @Test
    @DisplayName("CamelCase splits on whitespace, _, -, lower-upper and acronym boundaries")
    void camelCase() {
        CamelCaseTransform stage = CamelCaseTransform.of();
        assertThat(stage.execute(this.ctx, "hello world"), is(equalTo("helloWorld")));
        assertThat(stage.execute(this.ctx, "hello_world"), is(equalTo("helloWorld")));
        assertThat(stage.execute(this.ctx, "hello-world"), is(equalTo("helloWorld")));
        assertThat(stage.execute(this.ctx, "HelloWorld"), is(equalTo("helloWorld")));
        assertThat(stage.execute(this.ctx, "XMLParser"), is(equalTo("xmlParser")));
        assertThat(stage.execute(this.ctx, "single"), is(equalTo("single")));
        assertThat(stage.execute(this.ctx, ""), is(equalTo("")));
        assertThat(stage.execute(this.ctx, null), is(nullValue()));
    }

    @Test
    @DisplayName("PascalCase capitalises every word")
    void pascalCase() {
        PascalCaseTransform stage = PascalCaseTransform.of();
        assertThat(stage.execute(this.ctx, "hello world"), is(equalTo("HelloWorld")));
        assertThat(stage.execute(this.ctx, "hello_world"), is(equalTo("HelloWorld")));
        assertThat(stage.execute(this.ctx, "helloWorld"), is(equalTo("HelloWorld")));
        assertThat(stage.execute(this.ctx, "XMLParser"), is(equalTo("XmlParser")));
        assertThat(stage.execute(this.ctx, "single"), is(equalTo("Single")));
        assertThat(stage.execute(this.ctx, ""), is(equalTo("")));
        assertThat(stage.execute(this.ctx, null), is(nullValue()));
    }

    @Test
    @DisplayName("TitleCase uppercases word starts, lowercases everything else, preserves whitespace")
    void titleCase() {
        TitleCaseTransform stage = TitleCaseTransform.of();
        assertThat(stage.execute(this.ctx, "hello world"), is(equalTo("Hello World")));
        assertThat(stage.execute(this.ctx, "HELLO WORLD"), is(equalTo("Hello World")));
        assertThat(stage.execute(this.ctx, "  hello  world  "), is(equalTo("  Hello  World  ")));
        assertThat(stage.execute(this.ctx, "self-reliance"), is(equalTo("Self-reliance")));
        assertThat(stage.execute(this.ctx, ""), is(equalTo("")));
        assertThat(stage.execute(this.ctx, null), is(nullValue()));
    }

    @Test
    @DisplayName("SnakeCase joins lowercased words with underscores")
    void snakeCase() {
        SnakeCaseTransform stage = SnakeCaseTransform.of();
        assertThat(stage.execute(this.ctx, "hello world"), is(equalTo("hello_world")));
        assertThat(stage.execute(this.ctx, "HelloWorld"), is(equalTo("hello_world")));
        assertThat(stage.execute(this.ctx, "hello-world"), is(equalTo("hello_world")));
        assertThat(stage.execute(this.ctx, "hello_world"), is(equalTo("hello_world")));
        assertThat(stage.execute(this.ctx, "XMLParser"), is(equalTo("xml_parser")));
        assertThat(stage.execute(this.ctx, "single"), is(equalTo("single")));
        assertThat(stage.execute(this.ctx, ""), is(equalTo("")));
        assertThat(stage.execute(this.ctx, null), is(nullValue()));
    }

    @Test
    @DisplayName("ListLength returns the input list size")
    void listLength() {
        Integer size = SizeTransform.of(DataTypes.STRING).execute(this.ctx, List.of("a", "b", "c"));
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
