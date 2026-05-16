package dev.simplified.dataflow.stage.transform.json;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import dev.simplified.dataflow.DataType;
import dev.simplified.dataflow.DataTypes;
import dev.simplified.dataflow.PipelineContext;
import dev.simplified.dataflow.ValidationReport;
import dev.simplified.dataflow.chain.Chain;
import dev.simplified.dataflow.chain.ChainBuilder;
import dev.simplified.dataflow.chain.TypedChain;
import dev.simplified.dataflow.serde.PipelineGson;
import dev.simplified.dataflow.stage.TransformStage;
import dev.simplified.dataflow.stage.meta.Configurable;
import dev.simplified.dataflow.stage.meta.StageSpec;
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
@StageSpec(
    id = "TRANSFORM_JSON_OBJECT_BUILD",
    displayName = "JsonObject build",
    description = "I -> JSON_OBJECT",
    category = StageSpec.Category.TRANSFORM_JSON
)
@Getter
@Accessors(fluent = true)
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class ObjectBuildTransform<I> implements TransformStage<I, JsonObject> {

    private final @NotNull DataType<I> inputType;

    private final @NotNull Map<String, TypedChain<?>> outputs;

    /**
     * Mutable builder for {@link ObjectBuildTransform}.
     *
     * @param <I> input type, shared by every output's sub-chain
     */
    public static final class Builder<I> {

        private final @NotNull DataType<I> inputType;
        private final @NotNull Map<String, TypedChain<?>> outputs = new LinkedHashMap<>();

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
         * @param <O> the named output's declared output type
         */
        public <O> @NotNull Builder<I> output(
            @NotNull String name,
            @NotNull DataType<O> outputType,
            @NotNull Consumer<ChainBuilder<I, I>> block
        ) {
            ChainBuilder<I, I> builder = Chain.builder(this.inputType);
            block.accept(builder);
            Chain<I, O> chain = Chain.of(builder.build().stages());
            ValidationReport report = Chain.validate(this.inputType, chain.stages(), outputType);

            if (!report.isValid())
                throw new IllegalArgumentException("Invalid ObjectBuildTransform output '" + name + "': " + report.issues());

            this.outputs.put(name, new TypedChain<>(outputType, chain));
            return this;
        }

        /**
         * Builds the immutable {@link ObjectBuildTransform} stage.
         *
         * @return the built stage
         */
        public @NotNull ObjectBuildTransform<I> build() {
            return of(this.inputType, this.outputs);
        }

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

    /**
     * Canonical flat factory matching the wire shape. Constructs a {@link ObjectBuildTransform}
     * from its shared input type and a map of named, typed sub-chains.
     *
     * @param inputType the shared input type
     * @param outputs named sub-chains, each carrying its declared output {@link DataType}
     * @return the built transform
     * @param <I> input type
     */
    public static <I> @NotNull ObjectBuildTransform<I> of(
        @Configurable(label = "Input type", placeholder = "STRING")
        @NotNull DataType<I> inputType,
        @Configurable(label = "Outputs (typed)", placeholder = "")
        @NotNull Map<String, TypedChain<?>> outputs
    ) {
        return new ObjectBuildTransform<>(inputType, Map.copyOf(outputs));
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable JsonObject execute(@NotNull PipelineContext ctx, @Nullable I input) {
        if (input == null) return null;
        JsonObject result = new JsonObject();
        for (Map.Entry<String, TypedChain<?>> entry : this.outputs.entrySet()) {
            Object value = runBranch(entry.getValue(), ctx, input);
            if (value == null) continue;
            result.add(entry.getKey(), PipelineGson.gson().toJsonTree(value));
        }
        return result;
    }

    /**
     * Per-branch dispatch helper. {@link TypedChain#chain()} returns {@code Chain<?, O>} so
     * its input wildcard cannot be captured statically at the call site; the cast bridges
     * the enclosing transform's input {@code I} into the branch's unknown input slot. The
     * unchecked cast is sound because every branch is constructed by
     * {@link Builder#output} against {@code this.inputType} (which is {@code DataType<I>}),
     * so the branch's runtime input type is always {@code I}.
     *
     * @param typed the typed-chain branch
     * @param ctx the pipeline context
     * @param input the input value of type {@code I}
     * @return the branch's final value, or {@code null} when the chain rejected input
     */
    @SuppressWarnings("unchecked")
    private @Nullable Object runBranch(@NotNull TypedChain<?> typed, @NotNull PipelineContext ctx, @NotNull I input) {
        return ((Chain<I, ?>) typed.chain()).execute(ctx, input);
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
