package dev.simplified.dataflow.serde;

import dev.simplified.dataflow.DataTypes;
import dev.simplified.dataflow.chain.Chain;
import dev.simplified.dataflow.chain.NamedChains;
import dev.simplified.dataflow.chain.TypedChain;
import dev.simplified.dataflow.stage.Stage;
import dev.simplified.dataflow.stage.source.LiteralSource;
import dev.simplified.dataflow.stage.terminal.collect.MapCollect;
import dev.simplified.dataflow.stage.transform.json.ObjectBuildTransform;
import dev.simplified.dataflow.stage.transform.string.LengthTransform;
import dev.simplified.dataflow.stage.transform.string.UpperCaseTransform;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Covers the body validation {@link ObjectBuildTransform#of} and {@link MapCollect#of} perform,
 * on the flat factory and on the wire. Before it, both factories stored any body, so a
 * mis-typed body loaded from JSON and failed only when it ran.
 */
class BodyValidationTest {

    private static final @NotNull String SOURCE = "{\"kind\":\"SOURCE_LITERAL\",\"outputType\":\"STRING\",\"value\":\"abc\"}";

    private static @NotNull Throwable rootCause(@NotNull Throwable thrown) {
        Throwable root = thrown;
        while (root.getCause() != null && root.getCause() != root) root = root.getCause();
        return root;
    }

    private static @NotNull NamedChains<String> branch(@NotNull String name, @NotNull List<? extends Stage<?, ?>> body) {
        return NamedChains.of(Map.of(name, body));
    }

    @Test
    @DisplayName("ObjectBuildTransform.of accepts outputs whose bodies produce their declared types")
    void objectBuildAcceptsValidOutputs() {
        Map<String, TypedChain<?>> outputs = Map.of(
            "len", new TypedChain<>(DataTypes.INT, Chain.of(List.of(LengthTransform.of())))
        );
        assertThat(ObjectBuildTransform.of(DataTypes.STRING, outputs).outputs().size(), is(1));
    }

    @Test
    @DisplayName("ObjectBuildTransform.of rejects a body producing a type other than declared")
    void objectBuildRejectsDeclaredTypeMismatch() {
        Map<String, TypedChain<?>> outputs = Map.of(
            "n", new TypedChain<>(DataTypes.INT, Chain.of(List.of(UpperCaseTransform.of())))
        );
        IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class,
            () -> ObjectBuildTransform.of(DataTypes.STRING, outputs));
        assertThat(thrown.getMessage(), startsWith("Invalid ObjectBuildTransform output 'n'"));
    }

    @Test
    @DisplayName("ObjectBuildTransform.of rejects a body opening with a source")
    void objectBuildRejectsSourceInBody() {
        Map<String, TypedChain<?>> outputs = Map.of(
            "s", new TypedChain<>(DataTypes.STRING, Chain.of(List.of(LiteralSource.text("x"))))
        );
        assertThrows(IllegalArgumentException.class, () -> ObjectBuildTransform.of(DataTypes.STRING, outputs));
    }

    @Test
    @DisplayName("MapCollect.of accepts branches producing different types")
    void mapCollectAcceptsHeterogeneousBranches() {
        NamedChains<String> outputs = NamedChains.of(Map.of(
            "len", List.of(LengthTransform.of()),
            "upper", List.of(UpperCaseTransform.of())
        ));
        assertThat(MapCollect.of(DataTypes.STRING, outputs).outputs().size(), is(2));
    }

    @Test
    @DisplayName("MapCollect.of rejects a branch whose type chain breaks")
    void mapCollectRejectsBrokenChain() {
        NamedChains<String> outputs = branch("n", List.of(LengthTransform.of(), UpperCaseTransform.of()));
        IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class,
            () -> MapCollect.of(DataTypes.STRING, outputs));
        assertThat(thrown.getMessage(), startsWith("Invalid MapCollect output 'n'"));
    }

    @Test
    @DisplayName("MapCollect.of rejects a branch that does not consume the input type")
    void mapCollectRejectsWrongSeed() {
        NamedChains<Integer> outputs = NamedChains.of(Map.of("n", List.of(UpperCaseTransform.of())));
        assertThrows(IllegalArgumentException.class, () -> MapCollect.of(DataTypes.INT, outputs));
    }

    @Test
    @DisplayName("MapCollect.of rejects a branch opening with a source")
    void mapCollectRejectsSourceInBody() {
        NamedChains<String> outputs = branch("s", List.of(LiteralSource.text("x")));
        assertThrows(IllegalArgumentException.class, () -> MapCollect.of(DataTypes.STRING, outputs));
    }

    @Test
    @DisplayName("MapCollect.of rejects an empty branch")
    void mapCollectRejectsEmptyBranch() {
        NamedChains<String> outputs = branch("e", List.of());
        assertThrows(IllegalArgumentException.class, () -> MapCollect.of(DataTypes.STRING, outputs));
    }

    @Test
    @DisplayName("A wire ObjectBuild output declaring INT over a STRING body fails at load")
    void wireObjectBuildMismatchFailsAtLoad() {
        String json = "[" + SOURCE + ",{\"kind\":\"TRANSFORM_JSON_OBJECT_BUILD\",\"inputType\":\"STRING\",\"outputs\":"
            + "{\"n\":{\"outputType\":\"INT\",\"chain\":[{\"kind\":\"TRANSFORM_UPPERCASE\"}]}}}]";
        RuntimeException thrown = assertThrows(RuntimeException.class, () -> PipelineGson.fromJson(json));
        assertThat(rootCause(thrown).getMessage(), startsWith("Invalid ObjectBuildTransform output 'n'"));
    }

    @Test
    @DisplayName("A wire ObjectBuild output opening with a source fails at load")
    void wireObjectBuildSourceFailsAtLoad() {
        String json = "[" + SOURCE + ",{\"kind\":\"TRANSFORM_JSON_OBJECT_BUILD\",\"inputType\":\"STRING\",\"outputs\":"
            + "{\"s\":{\"outputType\":\"STRING\",\"chain\":[" + SOURCE + "]}}}]";
        RuntimeException thrown = assertThrows(RuntimeException.class, () -> PipelineGson.fromJson(json));
        assertThat(rootCause(thrown), is(instanceOf(IllegalArgumentException.class)));
    }

    @Test
    @DisplayName("A wire MapCollect branch opening with a source fails at load")
    void wireMapCollectSourceFailsAtLoad() {
        String json = "[" + SOURCE + ",{\"kind\":\"COLLECT_MAP\",\"inputType\":\"STRING\",\"outputs\":"
            + "{\"s\":[" + SOURCE + "]}}]";
        RuntimeException thrown = assertThrows(RuntimeException.class, () -> PipelineGson.fromJson(json));
        assertThat(rootCause(thrown).getMessage(), startsWith("Invalid MapCollect output 's'"));
    }

    @Test
    @DisplayName("A well-typed wire ObjectBuild still loads and runs")
    void wireValidObjectBuildLoads() {
        String json = "[" + SOURCE + ",{\"kind\":\"TRANSFORM_JSON_OBJECT_BUILD\",\"inputType\":\"STRING\",\"outputs\":"
            + "{\"n\":{\"outputType\":\"INT\",\"chain\":[{\"kind\":\"TRANSFORM_STRING_LENGTH\"}]}}}]";
        assertThat(PipelineGson.fromJson(json).execute().toString(), is(equalTo("{\"n\":3}")));
    }

}
