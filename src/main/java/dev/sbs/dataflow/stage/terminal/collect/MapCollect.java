package dev.sbs.dataflow.stage.terminal.collect;

import dev.sbs.dataflow.DataType;
import dev.sbs.dataflow.DataTypes;
import dev.sbs.dataflow.PipelineContext;
import dev.sbs.dataflow.stage.CollectStage;
import dev.sbs.dataflow.stage.Stage;
import dev.sbs.dataflow.stage.StageConfig;
import dev.sbs.dataflow.stage.StageKind;
import dev.simplified.collection.Concurrent;
import dev.simplified.collection.ConcurrentList;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Consumer;

/**
 * Terminal {@link CollectStage} that runs each named sub-chain against the shared input
 * value and returns the per-output results as an opaque {@code Map<String, Object>}.
 * <p>
 * Sub-chains are flat lists of {@link Stage} instances - they share the collect's input
 * type but otherwise have no source. Each sub-chain runs to completion (or until a stage
 * returns {@code null}), and its final value lands in the returned map under its name.
 *
 * @param <I> input type, shared by every sub-chain
 */
@Getter
@Accessors(fluent = true)
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class MapCollect<I> implements CollectStage<I, Map<String, Object>> {

    private final @NotNull DataType<I> inputType;

    private final @NotNull Map<String, ConcurrentList<Stage<?, ?>>> outputs;

    /**
     * Mutable builder for {@link MapCollect}.
     *
     * @param <I> input type, shared by every output's sub-chain
     */
    public static final class Builder<I> {
        private final @NotNull DataType<I> inputType;
        private final @NotNull Map<String, ConcurrentList<Stage<?, ?>>> outputs = new LinkedHashMap<>();

        private Builder(@NotNull DataType<I> inputType) {
            this.inputType = inputType;
        }

        /**
         * Adds a named output whose sub-chain is configured by the supplied block.
         *
         * @param name the output name
         * @param block builder block configuring the sub-chain
         * @return this builder
         */
        public @NotNull Builder<I> output(@NotNull String name, @NotNull Consumer<ChainBuilder> block) {
            ChainBuilder chain = new ChainBuilder();
            block.accept(chain);
            this.outputs.put(name, Concurrent.newUnmodifiableList(chain.stages));
            return this;
        }

        /**
         * Builds the immutable {@link MapCollect} stage.
         *
         * @return the built collect
         */
        public @NotNull MapCollect<I> build() {
            return new MapCollect<>(this.inputType, Map.copyOf(this.outputs));
        }
    }

    /**
     * Mutable builder for one named sub-chain inside a {@link MapCollect}.
     */
    public static final class ChainBuilder {
        private final @NotNull ConcurrentList<Stage<?, ?>> stages = Concurrent.newList();

        private ChainBuilder() {}

        /**
         * Appends a stage to this sub-chain.
         *
         * @param stage the stage to append
         * @return this builder
         */
        public @NotNull ChainBuilder stage(@NotNull Stage<?, ?> stage) {
            this.stages.add(stage);
            return this;
        }
    }

    /**
     * Reconstructs a {@link MapCollect} from a populated {@link StageConfig}.
     *
     * @param cfg the populated configuration
     * @return the rebuilt collect
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public static @NotNull MapCollect<?> fromConfig(@NotNull StageConfig cfg) {
        DataType<?> inputType = cfg.getDataType("inputType");
        Map<String, ConcurrentList<Stage<?, ?>>> outputs = cfg.getSubPipelines("outputs");
        return new MapCollect(inputType, outputs);
    }

    /**
     * Creates a fresh {@link Builder} for a collect keyed on the given input type.
     *
     * @param inputType the shared input type
     * @return a new builder
     * @param <I> input type
     */
    public static <I> @NotNull Builder<I> over(@NotNull DataType<I> inputType) {
        return new Builder<>(inputType);
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull StageConfig config() {
        return StageConfig.builder()
            .dataType("inputType", this.inputType)
            .subPipelines("outputs", this.outputs)
            .build();
    }

    /** {@inheritDoc} */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public @NotNull Map<String, Object> execute(@NotNull PipelineContext ctx, @Nullable I input) {
        Map<String, Object> result = new LinkedHashMap<>();
        for (Map.Entry<String, ConcurrentList<Stage<?, ?>>> entry : this.outputs.entrySet()) {
            Object current = input;
            for (Stage stage : entry.getValue()) {
                if (current == null) break;
                current = stage.execute(ctx, current);
            }
            result.put(entry.getKey(), current);
        }
        return Map.copyOf(result);
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull StageKind kind() {
        return StageKind.COLLECT_MAP;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<Map<String, Object>> outputType() {
        return DataTypes.MAP_OUTPUT;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull String summary() {
        return "MapCollect (" + this.outputs.size() + " outputs)";
    }

}
