package dev.sbs.dataflow.stage.transform.json;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import dev.sbs.dataflow.DataType;
import dev.sbs.dataflow.DataTypes;
import dev.sbs.dataflow.PipelineContext;
import dev.sbs.dataflow.StageChainValidator;
import dev.sbs.dataflow.ValidationReport;
import dev.sbs.dataflow.serde.PipelineGson;
import dev.sbs.dataflow.stage.Stage;
import dev.sbs.dataflow.stage.StageConfig;
import dev.sbs.dataflow.stage.StageConfig.TypedSubPipeline;
import dev.sbs.dataflow.stage.StageKind;
import dev.sbs.dataflow.stage.TransformStage;
import dev.simplified.collection.Concurrent;
import dev.simplified.collection.ConcurrentList;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Consumer;

/**
 * {@link TransformStage} that builds a {@link JsonObject} by fanning the input value out to
 * several named sub-pipelines and assembling their results as fields.
 * <p>
 * Each named output declares an explicit output {@link DataType}; the body chain is
 * validated against {@code (inputType, branchOutputType)} at build time. At execute time,
 * each branch's final value is coerced to a {@link JsonElement} via
 * {@link PipelineGson#gson() gson.toJsonTree(...)} and stored under its name. Null branch
 * results omit the field.
 *
 * @param <I> input type, shared by every named sub-pipeline
 */
@Getter
@Accessors(fluent = true)
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class JsonObjectBuildTransform<I> implements TransformStage<I, JsonObject> {

    private final @NotNull DataType<I> inputType;

    private final @NotNull Map<String, TypedSubPipeline> outputs;

    /**
     * Mutable builder for {@link JsonObjectBuildTransform}.
     *
     * @param <I> input type, shared by every output's sub-chain
     */
    public static final class Builder<I> {

        private final @NotNull DataType<I> inputType;
        private final @NotNull Map<String, TypedSubPipeline> outputs = new LinkedHashMap<>();

        private Builder(@NotNull DataType<I> inputType) {
            this.inputType = inputType;
        }

        /**
         * Adds a named output whose sub-chain produces {@code outputType}.
         *
         * @param name the output name
         * @param outputType the declared output {@link DataType} of the body's last stage
         * @param block builder block configuring the sub-chain
         * @return this builder
         */
        public @NotNull Builder<I> output(
            @NotNull String name,
            @NotNull DataType<?> outputType,
            @NotNull Consumer<ChainBuilder> block
        ) {
            ChainBuilder chain = new ChainBuilder();
            block.accept(chain);
            ValidationReport report = StageChainValidator.validate(this.inputType, chain.stages, outputType);

            if (!report.isValid())
                throw new IllegalArgumentException("Invalid JsonObjectBuildTransform output '" + name + "': " + report.issues());

            this.outputs.put(name, new TypedSubPipeline(outputType, Concurrent.newUnmodifiableList(chain.stages)));
            return this;
        }

        /**
         * Builds the immutable {@link JsonObjectBuildTransform} stage.
         *
         * @return the built stage
         */
        public @NotNull JsonObjectBuildTransform<I> build() {
            return new JsonObjectBuildTransform<>(this.inputType, Map.copyOf(this.outputs));
        }

    }

    /**
     * Mutable builder for one named sub-chain inside a {@link JsonObjectBuildTransform}.
     */
    @NoArgsConstructor(access = AccessLevel.PRIVATE)
    public static final class ChainBuilder {

        private final @NotNull ConcurrentList<Stage<?, ?>> stages = Concurrent.newList();

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
     * Reconstructs a {@link JsonObjectBuildTransform} from a populated {@link StageConfig}.
     *
     * @param cfg the populated configuration
     * @return the rebuilt stage
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public static @NotNull JsonObjectBuildTransform<?> fromConfig(@NotNull StageConfig cfg) {
        DataType<?> inputType = cfg.getDataType("inputType");
        Map<String, TypedSubPipeline> outputs = cfg.getTypedSubPipelines("outputs");
        return new JsonObjectBuildTransform(inputType, outputs);
    }

    /**
     * Creates a fresh {@link Builder} for a transform keyed on the given input type.
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
            .typedSubPipelines("outputs", this.outputs)
            .build();
    }

    /** {@inheritDoc} */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public @Nullable JsonObject execute(@NotNull PipelineContext ctx, @Nullable I input) {
        if (input == null) return null;
        JsonObject result = new JsonObject();
        for (Map.Entry<String, TypedSubPipeline> entry : this.outputs.entrySet()) {
            Object current = input;
            for (Stage stage : entry.getValue().chain()) {
                if (current == null) break;
                current = stage.execute(ctx, current);
            }
            if (current == null) continue;
            result.add(entry.getKey(), PipelineGson.gson().toJsonTree(current));
        }
        return result;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull StageKind kind() {
        return StageKind.TRANSFORM_JSON_OBJECT_BUILD;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<JsonObject> outputType() {
        return DataTypes.JSON_OBJECT;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull String summary() {
        return "JsonObject build (" + this.outputs.size() + " fields)";
    }

}
