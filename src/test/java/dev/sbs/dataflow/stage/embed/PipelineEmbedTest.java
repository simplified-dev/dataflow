package dev.sbs.dataflow.stage.embed;

import dev.sbs.dataflow.DataPipeline;
import dev.sbs.dataflow.DataPipelineResolver;
import dev.sbs.dataflow.DataTypes;
import dev.sbs.dataflow.PipelineContext;
import dev.sbs.dataflow.stage.source.PasteSource;
import dev.sbs.dataflow.stage.transform.TransformParseInt;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

class PipelineEmbedTest {

    /** Map-backed resolver that captures pipeline ids for cycle detection. */
    private static final class MapResolver implements DataPipelineResolver {
        final Map<String, DataPipeline> pipelines = new HashMap<>();

        @Override
        public @NotNull Optional<DataPipeline> resolve(@NotNull String id) {
            return Optional.ofNullable(this.pipelines.get(id));
        }

        @Override
        public @Nullable String idOf(@NotNull DataPipeline pipeline) {
            for (Map.Entry<String, DataPipeline> e : this.pipelines.entrySet()) {
                if (e.getValue() == pipeline) return e.getKey();
            }
            return null;
        }
    }

    @Test
    @DisplayName("Nested embed: pipeline A embeds pipeline B and runs it to completion")
    void nestedEmbedRunsInner() {
        MapResolver resolver = new MapResolver();
        // B: produces integer 42
        DataPipeline b = DataPipeline.builder()
            .source(PasteSource.text("42"))
            // PasteSource.text emits RAW_TEXT, but TransformParseInt expects STRING.
            // For now the embed test focuses on the embed mechanics, so we use a
            // simple chain: paste TEXT then we accept it as-is via a no-op chain.
            .build();
        resolver.pipelines.put("B", b);

        DataPipeline outer = DataPipeline.builder()
            .source(PipelineEmbed.of("B", DataTypes.RAW_TEXT))
            .build();

        PipelineContext ctx = PipelineContext.builder().withResolver(resolver).build();
        Object result = outer.execute(ctx);
        assertThat(result, is(equalTo("42")));
    }

    @Test
    @DisplayName("Self-cycle: A embeds A throws with breadcrumb")
    void selfCycleDetected() {
        MapResolver resolver = new MapResolver();
        DataPipeline a = DataPipeline.builder()
            .source(PipelineEmbed.of("A", DataTypes.RAW_TEXT))
            .build();
        resolver.pipelines.put("A", a);

        PipelineContext ctx = PipelineContext.builder().withResolver(resolver).build();

        try {
            a.execute(ctx);
        } catch (IllegalStateException expected) {
            assertThat(expected.getMessage(), containsString("Pipeline cycle detected"));
            assertThat(expected.getMessage(), containsString("A"));
            return;
        }
        throw new AssertionError("Expected IllegalStateException for self-cycle");
    }

    @Test
    @DisplayName("Mutual cycle: A -> B -> A throws with both ids in breadcrumb")
    void mutualCycleDetected() {
        MapResolver resolver = new MapResolver();

        DataPipeline a = DataPipeline.builder()
            .source(PipelineEmbed.of("B", DataTypes.RAW_TEXT))
            .build();
        DataPipeline b = DataPipeline.builder()
            .source(PipelineEmbed.of("A", DataTypes.RAW_TEXT))
            .build();
        resolver.pipelines.put("A", a);
        resolver.pipelines.put("B", b);

        PipelineContext ctx = PipelineContext.builder().withResolver(resolver).build();

        try {
            a.execute(ctx);
        } catch (IllegalStateException expected) {
            assertThat(expected.getMessage(), containsString("Pipeline cycle detected"));
            assertThat(expected.getMessage(), containsString("A"));
            assertThat(expected.getMessage(), containsString("B"));
            return;
        }
        throw new AssertionError("Expected IllegalStateException for mutual cycle");
    }

    @Test
    @DisplayName("Missing pipeline id throws a clear error")
    void missingPipelineThrows() {
        MapResolver resolver = new MapResolver();
        DataPipeline a = DataPipeline.builder()
            .source(PipelineEmbed.of("does-not-exist", DataTypes.RAW_TEXT))
            .build();

        PipelineContext ctx = PipelineContext.builder().withResolver(resolver).build();

        try {
            a.execute(ctx);
        } catch (IllegalStateException expected) {
            assertThat(expected.getMessage(), containsString("does-not-exist"));
            return;
        }
        throw new AssertionError("Expected IllegalStateException for missing pipeline");
    }

    @Test
    @DisplayName("PipelineEmbed is a valid first stage of a DataPipeline")
    void embedIsValidFirstStage() {
        DataPipeline outer = DataPipeline.builder()
            .source(PipelineEmbed.of("X", DataTypes.STRING))
            .build();
        assertThat(outer.validate().isValid(), is(true));
    }

    @Test
    @DisplayName("Three-deep embed A -> B -> C, no cycle, returns inner result")
    void threeDeepNonCycle() {
        MapResolver resolver = new MapResolver();
        DataPipeline c = DataPipeline.builder()
            .source(PasteSource.text("deep"))
            .build();
        DataPipeline b = DataPipeline.builder()
            .source(PipelineEmbed.of("C", DataTypes.RAW_TEXT))
            .build();
        DataPipeline a = DataPipeline.builder()
            .source(PipelineEmbed.of("B", DataTypes.RAW_TEXT))
            .build();
        resolver.pipelines.put("A", a);
        resolver.pipelines.put("B", b);
        resolver.pipelines.put("C", c);

        PipelineContext ctx = PipelineContext.builder().withResolver(resolver).build();
        Object result = a.execute(ctx);
        assertThat(result, is(equalTo("deep")));
    }

}
