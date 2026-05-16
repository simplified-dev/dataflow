package dev.sbs.dataflow;

import dev.sbs.dataflow.stage.SourceStage;
import dev.sbs.dataflow.stage.Stage;
import dev.simplified.collection.Concurrent;
import dev.simplified.collection.ConcurrentList;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Immutable, validated sequence of {@link Stage} instances that maps a source-produced
 * value through a chain of filters / transforms / collectors to a final result.
 * <p>
 * A pipeline always begins with a {@link SourceStage}. Every subsequent stage's
 * {@link Stage#inputType() input type} must equal the previous stage's
 * {@link Stage#outputType() output type}; this is enforced by {@link #validate()} and
 * re-checked at runtime by {@link #execute(PipelineContext)}.
 */
@Getter
@Accessors(fluent = true)
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class DataPipeline {

    private static final @NotNull DataPipeline EMPTY = new DataPipeline(Concurrent.newUnmodifiableList());

    private final @NotNull ConcurrentList<Stage<?, ?>> stages;

    /**
     * Returns the canonical empty pipeline; useful as a starting point for builders.
     *
     * @return the empty pipeline
     */
    public static @NotNull DataPipeline empty() {
        return EMPTY;
    }

    /**
     * Creates a fresh pipeline {@link Builder}.
     *
     * @return a new builder
     */
    public static @NotNull Builder builder() {
        return new Builder();
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
     * Executes the pipeline against {@link PipelineContext#defaults()}, returning the final
     * value with the type inferred at the call site.
     * <p>
     * The cast from the runtime {@link Object} to {@code T} is unchecked - the compiler
     * trusts the inferred type. A mismatch surfaces as a {@link ClassCastException} at
     * the assignment site, the same way {@link Map#get(Object)} behaves with a
     * casted return.
     * <p>
     * Validation runs once at build time (see {@link Builder#build()}) so this method does
     * not re-validate before each execution.
     *
     * @return the final value, possibly {@code null} when a stage rejects its input
     * @param <T> inferred result type
     */
    public <T> @Nullable T execute() {
        return execute(PipelineContext.defaults());
    }

    /**
     * Executes the pipeline against {@code ctx}, returning the final value with the type
     * inferred at the call site.
     * <p>
     * The cast from the runtime {@link Object} to {@code T} is unchecked - the compiler
     * trusts the inferred type. A mismatch surfaces as a {@link ClassCastException} at
     * the assignment site, the same way {@link Map#get(Object)} behaves with a
     * casted return.
     * <p>
     * Validation runs once at build time (see {@link Builder#build()}) so this method does
     * not re-validate before each execution.
     *
     * @param ctx the pipeline context
     * @return the final value, possibly {@code null} when a stage rejects its input
     * @param <T> inferred result type
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public <T> @Nullable T execute(@NotNull PipelineContext ctx) {
        Object current = null;

        for (Stage stage : this.stages) {
            current = stage.execute(ctx, current);
            ctx.traceStage(stage, current);
            if (current == null) break;
        }

        return (T) current;
    }

    /**
     * Mutable builder for assembling a {@link DataPipeline} from {@link Stage} instances.
     */
    @NoArgsConstructor(access = AccessLevel.PRIVATE)
    public static final class Builder {

        private final @NotNull ConcurrentList<Stage<?, ?>> stages = Concurrent.newList();

        /**
         * Sets the source stage. Must be called exactly once before {@link #build()}.
         * Accepts any {@link SourceStage}, including the special {@code EmbedSource}
         * source that delegates to a saved pipeline.
         *
         * @param source the first stage
         * @return this builder
         */
        public @NotNull Builder source(@NotNull SourceStage<?> source) {
            if (!this.stages.isEmpty())
                throw new IllegalStateException("Source already set");

            this.stages.add(source);
            return this;
        }

        /**
         * Appends a non-source stage to the chain.
         *
         * @param stage the stage to append
         * @return this builder
         */
        public @NotNull Builder stage(@NotNull Stage<?, ?> stage) {
            this.stages.add(stage);
            return this;
        }

        /**
         * Returns a validation report for the stages staged so far without building. Useful
         * for inspecting type-chain errors while a pipeline is still under construction.
         *
         * @return the validation report
         */
        public @NotNull ValidationReport validate() {
            return new DataPipeline(Concurrent.newUnmodifiableList(this.stages)).validate();
        }

        /**
         * Builds an immutable {@link DataPipeline} from the staged stages, validating the
         * type chain eagerly. {@link DataPipeline#execute(PipelineContext)} relies on this
         * pre-validation to skip per-run checks.
         *
         * @return the built pipeline
         * @throws IllegalStateException when the type chain has any issues
         */
        public @NotNull DataPipeline build() {
            DataPipeline pipeline = new DataPipeline(Concurrent.newUnmodifiableList(this.stages));
            ValidationReport report = pipeline.validate();

            if (!report.isValid())
                throw new IllegalStateException("Cannot build invalid pipeline: " + report.issues());

            return pipeline;
        }

    }

}
