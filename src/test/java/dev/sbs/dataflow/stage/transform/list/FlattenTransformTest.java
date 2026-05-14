package dev.sbs.dataflow.stage.transform.list;

import dev.sbs.dataflow.DataTypes;
import dev.sbs.dataflow.PipelineContext;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

class FlattenTransformTest {

    private final PipelineContext ctx = PipelineContext.empty();

    @Test
    @DisplayName("Flatten concatenates inner lists in order")
    void flattenConcatenates() {
        FlattenTransform<String> flatten = FlattenTransform.of(DataTypes.STRING);
        List<String> result = flatten.execute(this.ctx, List.of(
            List.of("a", "b"),
            List.of("c"),
            List.of("d", "e", "f")
        ));
        assertThat(result, contains("a", "b", "c", "d", "e", "f"));
    }

    @Test
    @DisplayName("Flatten returns null on null input")
    void flattenNullInputProducesNull() {
        FlattenTransform<String> flatten = FlattenTransform.of(DataTypes.STRING);
        assertThat(flatten.execute(this.ctx, null), is(nullValue()));
    }

    @Test
    @DisplayName("Flatten skips null inner sub-lists")
    void flattenSkipsNullInner() {
        FlattenTransform<String> flatten = FlattenTransform.of(DataTypes.STRING);
        List<String> result = flatten.execute(this.ctx, Arrays.asList(
            List.of("a"),
            null,
            List.of("b", "c")
        ));
        assertThat(result, contains("a", "b", "c"));
    }

    @Test
    @DisplayName("Flatten on empty list yields empty list")
    void flattenEmptyInputProducesEmpty() {
        FlattenTransform<String> flatten = FlattenTransform.of(DataTypes.STRING);
        assertThat(flatten.execute(this.ctx, List.of()), is(List.of()));
    }

}
