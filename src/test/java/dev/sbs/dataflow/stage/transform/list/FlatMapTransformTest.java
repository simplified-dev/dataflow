package dev.sbs.dataflow.stage.transform.list;

import dev.sbs.dataflow.DataTypes;
import dev.sbs.dataflow.PipelineContext;
import dev.sbs.dataflow.stage.transform.string.SplitTransform;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;

class FlatMapTransformTest {

    private final PipelineContext ctx = PipelineContext.empty();

    @Test
    @DisplayName("FlatMap concatenates each element's per-body output list")
    void flatMapConcatenatesResults() {
        FlatMapTransform<String, String> flat = FlatMapTransform.of(
            DataTypes.STRING,
            DataTypes.STRING,
            List.of(SplitTransform.of(","))
        );
        List<String> result = flat.execute(this.ctx, List.of("a,b", "c", "d,e,f"));
        assertThat(result, contains("a", "b", "c", "d", "e", "f"));
    }

    @Test
    @DisplayName("FlatMap returns null on null input")
    void flatMapNullInputProducesNull() {
        FlatMapTransform<String, String> flat = FlatMapTransform.of(
            DataTypes.STRING,
            DataTypes.STRING,
            List.of(SplitTransform.of(","))
        );
        assertThat(flat.execute(this.ctx, null), is(nullValue()));
    }

    @Test
    @DisplayName("FlatMap on empty input yields empty list")
    void flatMapEmptyInputProducesEmpty() {
        FlatMapTransform<String, String> flat = FlatMapTransform.of(
            DataTypes.STRING,
            DataTypes.STRING,
            List.of(SplitTransform.of(","))
        );
        assertThat(flat.execute(this.ctx, List.of()), is(List.of()));
    }

    @Test
    @DisplayName("FlatMap rejects a body whose final output type is not a list of the declared element type")
    void flatMapRejectsScalarBody() {
        assertThrows(IllegalArgumentException.class, () -> FlatMapTransform.of(
            DataTypes.STRING,
            DataTypes.STRING,
            List.of()
        ));
    }

}
