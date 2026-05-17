package dev.simplified.dataflow;

import dev.simplified.collection.Concurrent;
import dev.simplified.collection.ConcurrentList;
import dev.simplified.dataflow.stage.SourceStage;
import dev.simplified.dataflow.stage.Stage;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.List;

/**
 * Immutable, validated sequence of {@link Stage} instances that maps a source-produced
 * value through a chain of filters / transforms / collectors to a final result of type
 * {@code O}.
 * <p>
 * A pipeline always begins with a {@link SourceStage}. Every subsequent stage's
 * {@link Stage#inputType() input type} must equal the previous stage's
 * {@link Stage#outputType() output type}; this is enforced by {@link #validate()} and
 * checked at compile time on the typed builder path.
 *
 * @param <O> output type of the pipeline's last stage
 */
@Getter
@Accessors(fluent = true)
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class DataPipeline<O> {

    private static final @NotNull DataPipeline<Void> EMPTY =
        new DataPipeline<>(Concurrent.newUnmodifiableList(), DataTypes.NONE);

    private final @NotNull ConcurrentList<Stage<?, ?>> stages;

    private final @NotNull DataType<O> outputType;

    /**
     * Returns the canonical empty pipeline; useful as a starting point for builders.
     *
     * @return the empty pipeline
     */
    public static @NotNull DataPipeline<Void> empty() {
        return EMPTY;
    }

    /**
     * Creates a fresh {@link Origin} for assembling a pipeline starting from a
     * {@link SourceStage}.
     *
     * @return a new origin
     */
    public static @NotNull Origin builder() {
        return new Origin();
    }

    /**
     * Serde-only entry that bypasses the typed builder. The caller asserts that
     * {@code stages} forms a well-typed chain whose final stage produces {@code outputType};
     * the contract is checked dynamically via {@link #validate()} on the
     * {@link dev.simplified.dataflow.serde.PipelineGson} path. Not intended for general use -
     * the {@link Builder} path enforces the same contract at compile time.
     *
     * @param stages the assembled stages in execution order
     * @param outputType the declared output type of the last stage
     * @return the constructed pipeline
     * @param <O> declared output type
     */
    public static <O> @NotNull DataPipeline<O> unchecked(
        @NotNull List<Stage<?, ?>> stages,
        @NotNull DataType<O> outputType
    ) {
        return new DataPipeline<>(Concurrent.newUnmodifiableList(stages), outputType);
    }

    /**
     * Walks the stage chain and reports every type-chain mismatch and structural issue.
     * <p>
     * Pipelines with zero stages report a single pipeline-level "missing source" issue.
     * Pipelines whose first stage expects an upstream input report a structural issue.
     *
     * @return the validation report
     */
    public @NotNull ValidationReport validate() {
        if (this.stages.isEmpty())
            return ValidationReport.of(ValidationReport.Issue.pipelineLevel(
                "Pipeline has no stages; expected at least a source stage"
            ));

        List<ValidationReport.Issue> issues = new ArrayList<>();
        Stage<?, ?> first = this.stages.getFirst();

        if (!(first instanceof SourceStage<?>))
            issues.add(new ValidationReport.Issue(0,
                "First stage must be a SourceStage but was " + first.getClass().getSimpleName()
            ));

        DataType<?> previousOutput = first.outputType();
        for (int i = 1; i < this.stages.size(); i++) {
            Stage<?, ?> stage = this.stages.get(i);
            DataType<?> expected = stage.inputType();

            if (!expected.equals(previousOutput)) {
                issues.add(new ValidationReport.Issue(
                    i,
                    "Stage #" + i + " (" + stage.kindId() + ") expects input " + expected
                        + " but previous stage produced " + previousOutput
                ));
            }

            previousOutput = stage.outputType();
        }

        return new ValidationReport(List.copyOf(issues));
    }

    /**
     * Executes the pipeline against {@link PipelineContext#defaults()}.
     * <p>
     * Validation runs once at build time (see {@link Builder#build()}) so this method does
     * not re-validate before each execution.
     *
     * @return the final value, possibly {@code null} when a stage rejects its input
     */
    public @Nullable O execute() {
        return execute(PipelineContext.defaults());
    }

    /**
     * Executes the pipeline against {@code ctx}.
     * <p>
     * Validation runs once at build time (see {@link Builder#build()}) so this method does
     * not re-validate before each execution.
     *
     * @param ctx the pipeline context
     * @return the final value, possibly {@code null} when a stage rejects its input
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public @Nullable O execute(@NotNull PipelineContext ctx) {
        Object current = null;

        for (Stage stage : this.stages) {
            current = stage.execute(ctx, current);
            ctx.traceStage(stage, current);
            if (current == null) break;
        }

        return (O) current;
    }

    /**
     * Narrows this pipeline to one whose static output type is {@code type}, verifying the
     * runtime output type matches. Used by callers of the deserialisation path to recover a
     * typed handle from the wildcard pipeline returned by
     * {@link dev.simplified.dataflow.serde.PipelineGson#fromJson(String)}.
     *
     * @param type the expected output type
     * @return this pipeline, narrowed to produce {@code T}
     * @param <T> the expected output type
     * @throws IllegalStateException when the runtime output type does not equal {@code type}
     */
    @SuppressWarnings("unchecked")
    public <T> @NotNull DataPipeline<T> expectOutput(@NotNull DataType<T> type) {
        if (!this.outputType.equals(type))
            throw new IllegalStateException(
                "expected output type " + type + " but pipeline produces " + this.outputType
            );
        return (DataPipeline<T>) this;
    }

    /**
     * Typed-factory shim returned by {@link DataPipeline#builder()}. Hosts a single
     * {@link #source} method whose generic parameter fixes the pipeline's running type
     * at the source's output type, transitioning to {@link Builder}.
     */
    @NoArgsConstructor(access = AccessLevel.PRIVATE)
    public static final class Origin {

        /**
         * Sets the source stage and transitions to the typed builder phase.
         *
         * @param source the first stage
         * @return a typed builder running on the source's output type
         * @param <T> the source's output type
         */
        public <T> @NotNull Builder<T> source(@NotNull SourceStage<T> source) {
            List<Stage<?, ?>> stages = new ArrayList<>();
            stages.add(source);
            return new Builder<>(stages, source.outputType());
        }

    }

    /**
     * Typed builder phase whose phantom type parameter tracks the running output type of
     * the staged chain. Each {@code stage} call advances the phantom type to the new
     * stage's output type.
     *
     * @param <T> running output type of the staged chain
     */
    public static final class Builder<T> {

        private final @NotNull List<Stage<?, ?>> stages;
        private @NotNull DataType<T> currentOutputType;

        private Builder(@NotNull List<Stage<?, ?>> stages, @NotNull DataType<T> currentOutputType) {
            this.stages = stages;
            this.currentOutputType = currentOutputType;
        }

        /**
         * Appends a non-source stage to the chain, advancing the running output type to
         * {@code U}.
         *
         * @param stage the stage to append, whose input must be assignable from {@code T}
         *              and whose output is {@code U}
         * @return this builder, retyped to run on {@code U}
         * @param <U> the appended stage's output type
         */
        @SuppressWarnings("unchecked")
        public <U> @NotNull Builder<U> stage(@NotNull Stage<? super T, ? extends U> stage) {
            this.stages.add(stage);
            Builder<U> cast = (Builder<U>) this;
            cast.currentOutputType = (DataType<U>) stage.outputType();
            return cast;
        }

        /**
         * Returns a validation report for the stages staged so far without throwing. Useful
         * for inspecting type-chain errors while a pipeline is still under construction.
         *
         * @return the validation report
         */
        public @NotNull ValidationReport validate() {
            return new DataPipeline<>(
                Concurrent.newUnmodifiableList(this.stages), this.currentOutputType
            ).validate();
        }

        /**
         * Builds an immutable {@link DataPipeline} from the staged stages, validating the
         * type chain eagerly. {@link DataPipeline#execute(PipelineContext)} relies on this
         * pre-validation to skip per-run checks.
         *
         * @return the built pipeline
         * @throws IllegalStateException when the type chain has any issues
         */
        public @NotNull DataPipeline<T> build() {
            DataPipeline<T> pipeline = new DataPipeline<>(
                Concurrent.newUnmodifiableList(this.stages), this.currentOutputType
            );
            ValidationReport report = pipeline.validate();

            if (!report.isValid())
                throw new IllegalStateException("Cannot build invalid pipeline: " + report.issues());

            return pipeline;
        }

    }

}
