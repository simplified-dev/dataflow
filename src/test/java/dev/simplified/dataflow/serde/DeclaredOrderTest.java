package dev.simplified.dataflow.serde;

import com.google.gson.JsonObject;
import dev.simplified.dataflow.DataPipeline;
import dev.simplified.dataflow.DataTypes;
import dev.simplified.dataflow.PipelineContext;
import dev.simplified.dataflow.chain.NamedChains;
import dev.simplified.dataflow.stage.Stage;
import dev.simplified.dataflow.stage.predicate.common.AndPredicate;
import dev.simplified.dataflow.stage.predicate.string.NonEmptyPredicate;
import dev.simplified.dataflow.stage.source.LiteralSource;
import dev.simplified.dataflow.stage.terminal.collect.MapCollect;
import dev.simplified.dataflow.stage.transform.json.ObjectBuildTransform;
import dev.simplified.dataflow.stage.transform.string.LengthTransform;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Covers the declared order of ObjectBuild outputs, MapCollect results and named bodies, which
 * survives into execution and back onto the wire. Before, each was copied with
 * {@code Map.copyOf}, whose iteration order is unspecified.
 */
class DeclaredOrderTest {

    private static final @NotNull List<String> NAMES = List.of("m", "z", "a", "q", "b", "y", "c", "x");

    private static @NotNull ObjectBuildTransform<String> objectBuild() {
        ObjectBuildTransform.Builder<String> builder = ObjectBuildTransform.over(DataTypes.STRING);
        for (String name : NAMES)
            builder.output(name, DataTypes.INT, c -> c.stage(LengthTransform.of()));
        return builder.build();
    }

    private static @NotNull MapCollect<String> mapCollect() {
        MapCollect.Builder<String> builder = MapCollect.over(DataTypes.STRING);
        for (String name : NAMES)
            builder.output(name, c -> c.stage(LengthTransform.of()));
        return builder.build();
    }

    private static @NotNull Map<String, List<Stage<?, ?>>> bodies() {
        Map<String, List<Stage<?, ?>>> bodies = new LinkedHashMap<>();
        for (String name : NAMES)
            bodies.put(name, List.of(NonEmptyPredicate.of()));
        return bodies;
    }

    /**
     * Returns the positions of every name in {@code json}, in {@link #NAMES} order.
     */
    private static @NotNull List<Integer> positions(@NotNull String json) {
        return NAMES.stream().map(name -> json.indexOf("\"" + name + "\":")).toList();
    }

    @Test
    @DisplayName("ObjectBuild emits fields in the declared order")
    void objectBuildFieldOrder() {
        JsonObject result = objectBuild().execute(PipelineContext.defaults(), "abc");
        assertThat(List.copyOf(result.keySet()), is(equalTo(NAMES)));
    }

    @Test
    @DisplayName("ObjectBuild keeps its outputs in the declared order")
    void objectBuildOutputsOrder() {
        assertThat(List.copyOf(objectBuild().outputs().keySet()), is(equalTo(NAMES)));
    }

    @Test
    @DisplayName("ObjectBuild outputs are unmodifiable")
    void objectBuildOutputsUnmodifiable() {
        assertThrows(UnsupportedOperationException.class, () -> objectBuild().outputs().clear());
    }

    @Test
    @DisplayName("ObjectBuild outputs are written to the wire in the declared order")
    void objectBuildWireOrder() {
        String json = PipelineGson.toJson(DataPipeline.builder().source(LiteralSource.text("abc")).stage(objectBuild()).build());
        assertThat(positions(json), is(equalTo(positions(json).stream().sorted().toList())));
    }

    @Test
    @DisplayName("ObjectBuild outputs read from the wire keep the document order")
    void objectBuildWireReadOrder() {
        String json = PipelineGson.toJson(DataPipeline.builder().source(LiteralSource.text("abc")).stage(objectBuild()).build());
        ObjectBuildTransform<?> rebuilt = (ObjectBuildTransform<?>) PipelineGson.fromJson(json).stages().getLast();
        assertThat(List.copyOf(rebuilt.outputs().keySet()), is(equalTo(NAMES)));
    }

    @Test
    @DisplayName("MapCollect returns its results in the declared order")
    void mapCollectResultOrder() {
        Map<String, Object> result = mapCollect().execute(PipelineContext.defaults(), "abc");
        assertThat(List.copyOf(result.keySet()), is(equalTo(NAMES)));
    }

    @Test
    @DisplayName("MapCollect results are unmodifiable")
    void mapCollectResultUnmodifiable() {
        Map<String, Object> result = mapCollect().execute(PipelineContext.defaults(), "abc");
        assertThrows(UnsupportedOperationException.class, () -> result.put("k", 1));
    }

    @Test
    @DisplayName("MapCollect branches read from the wire keep the document order")
    void mapCollectWireReadOrder() {
        String json = PipelineGson.toJson(DataPipeline.builder().source(LiteralSource.text("abc")).stage(mapCollect()).build());
        MapCollect<?> rebuilt = (MapCollect<?>) PipelineGson.fromJson(json).stages().getLast();
        assertThat(List.copyOf(rebuilt.outputs().chains().keySet()), is(equalTo(NAMES)));
    }

    @Test
    @DisplayName("NamedChains.of keeps the declared order")
    void namedChainsOfOrder() {
        assertThat(List.copyOf(NamedChains.of(bodies()).chains().keySet()), is(equalTo(NAMES)));
    }

    @Test
    @DisplayName("Named predicate bodies keep the declared order through a wire round trip")
    void namedBodiesWireOrder() {
        String written = PipelineGson.toJson(DataPipeline.builder()
            .source(LiteralSource.text("abc"))
            .stage(AndPredicate.of(DataTypes.STRING, bodies()))
            .build());
        AndPredicate<?> rebuilt = (AndPredicate<?>) PipelineGson.fromJson(written).stages().getLast();
        assertThat(List.copyOf(rebuilt.bodies().chains().keySet()), is(equalTo(NAMES)));
    }

}
