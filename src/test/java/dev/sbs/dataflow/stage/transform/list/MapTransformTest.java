package dev.sbs.dataflow.stage.transform.list;

import dev.sbs.dataflow.DataTypes;
import dev.sbs.dataflow.PipelineContext;
import dev.sbs.dataflow.stage.transform.primitive.ParseIntTransform;
import dev.sbs.dataflow.stage.transform.string.TrimTransform;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;

class MapTransformTest {

    private final PipelineContext ctx = PipelineContext.defaults();

    @Test
    @DisplayName("Map runs the body once per element and collects results")
    void mapAppliesBodyPerElement() {
        MapTransform<String, Integer> map = MapTransform.of(
            DataTypes.STRING,
            DataTypes.INT,
            List.of(ParseIntTransform.of())
        );
        List<Integer> result = map.execute(this.ctx, List.of("1", "2", "3"));
        assertThat(result, contains(1, 2, 3));
    }

    @Test
    @DisplayName("Map drops elements whose body returns null")
    void mapDropsNullResults() {
        MapTransform<String, Integer> map = MapTransform.of(
            DataTypes.STRING,
            DataTypes.INT,
            List.of(ParseIntTransform.of())
        );
        List<Integer> result = map.execute(this.ctx, List.of("1", "not-a-number", "3"));
        assertThat(result, contains(1, 3));
    }

    @Test
    @DisplayName("Map returns null on null input")
    void mapNullInputProducesNull() {
        MapTransform<String, String> map = MapTransform.of(
            DataTypes.STRING,
            DataTypes.STRING,
            List.of(TrimTransform.of())
        );
        assertThat(map.execute(this.ctx, null), is(nullValue()));
    }

    @Test
    @DisplayName("Map supports multi-stage bodies")
    void mapChainsBodyStages() {
        MapTransform<String, Integer> map = MapTransform.of(
            DataTypes.STRING,
            DataTypes.INT,
            List.of(TrimTransform.of(), ParseIntTransform.of())
        );
        List<Integer> result = map.execute(this.ctx, List.of("  10  ", " 20", "30 "));
        assertThat(result, contains(10, 20, 30));
    }

    @Test
    @DisplayName("Map rejects a body whose final output type mismatches the declared element type")
    void mapRejectsBadBody() {
        assertThrows(IllegalArgumentException.class, () -> MapTransform.of(
            DataTypes.STRING,
            DataTypes.STRING,
            List.of(ParseIntTransform.of())
        ));
    }

    @Test
    @DisplayName("Map rejects an empty body")
    void mapRejectsEmptyBody() {
        assertThrows(IllegalArgumentException.class, () -> MapTransform.of(
            DataTypes.STRING,
            DataTypes.STRING,
            List.of()
        ));
    }

}
