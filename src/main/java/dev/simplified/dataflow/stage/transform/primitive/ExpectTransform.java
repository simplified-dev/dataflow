package dev.simplified.dataflow.stage.transform.primitive;

import dev.simplified.annotations.AccessLevel;
import dev.simplified.annotations.Getter;
import dev.simplified.annotations.NamingStyle;
import dev.simplified.annotations.RequiredArgsConstructor;
import dev.simplified.dataflow.DataPipeline;
import dev.simplified.dataflow.DataType;
import dev.simplified.dataflow.DataTypes;
import dev.simplified.dataflow.PipelineContext;
import dev.simplified.dataflow.ValidationReport;
import dev.simplified.dataflow.chain.Chain;
import dev.simplified.dataflow.exception.ExpectationFailedException;
import dev.simplified.dataflow.stage.Stage;
import dev.simplified.dataflow.stage.TransformStage;
import dev.simplified.dataflow.stage.meta.Configurable;
import dev.simplified.dataflow.stage.meta.StageSpec;
import dev.simplified.dataflow.stage.predicate.common.NotNullPredicate;
import dev.simplified.dataflow.stage.predicate.json.HasFieldPredicate;
import dev.simplified.dataflow.stage.source.EmbedSource;
import dev.simplified.dataflow.stage.transform.json.AsStringTransform;
import dev.simplified.dataflow.stage.transform.json.FieldTransform;
import dev.simplified.dataflow.stage.transform.json.PathTransform;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.List;

/**
 * Identity {@link TransformStage} that asserts an expectation about the value passing through
 * it, stated as a human sentence and tested by a predicate body.
 * <p>
 * The input is returned unchanged when the body yields {@code true}. When it yields
 * {@code false} or {@code null} the run stops with an {@link ExpectationFailedException} whose
 * message names the expectation, so a document that no longer has the shape a pipeline was
 * written against fails loudly rather than producing wrong rows. A {@code null} input is
 * passed through as {@code null} without running the body.
 * <p>
 * A value that is absent therefore never reaches the body, and an expect stage over it cannot
 * require it. Presence is stated on the value that holds it instead: a body that reads a missing
 * part out of its input yields {@code null}, and that verdict fails, so an expectation over each
 * row whose body reads its {@code id} fails on a row that has none.
 * <p>
 * A key holding JSON {@code null} is not missing. {@link PathTransform} and {@link FieldTransform}
 * read it as a JSON null element, which is a value, and {@link NotNullPredicate} and
 * {@link HasFieldPredicate} both answer {@code true} for it, so a body built of those stages
 * passes a row whose {@code id} is JSON {@code null}. A body that requires a non-null JSON
 * primitive reads it typed: {@link AsStringTransform}, like every {@code TRANSFORM_JSON_AS_*}
 * stage, yields {@code null} for JSON {@code null}, an object or an array, so an expectation whose
 * body is {@code TRANSFORM_JSON_PATH id}, {@code TRANSFORM_JSON_AS_STRING},
 * {@code PREDICATE_NOT_NULL} fails on a row whose {@code id} is missing or JSON {@code null}.
 * <p>
 * The expectation is known before any run: {@link DataPipeline#validate()} lists it in
 * {@link ValidationReport#expectations()} with the path of the stage, wherever the stage sits in
 * the pipeline - at the top level, in a body, or in a pipeline operand - and stating one never
 * makes a pipeline invalid. A saved pipeline an {@link EmbedSource} runs is resolved only when the
 * embed executes, so an expectation inside it is listed by validating the resolved pipeline, not
 * the pipeline that embeds it.
 *
 * @param <T> value type
 */
@StageSpec(
    id = "TRANSFORM_EXPECT",
    displayName = "Expect",
    description = "T -> T (body: T -> BOOLEAN)",
    category = StageSpec.Category.TRANSFORM_PRIMITIVE
)
@Getter(style = NamingStyle.FLUENT)
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class ExpectTransform<T> implements TransformStage<T, T> {

    private static final int PREVIEW_LENGTH = 120;

    /**
     * Type of the value flowing through this stage, which the body consumes.
     */
    private final @NotNull DataType<T> inputType;

    /**
     * Human sentence stating what the body checks, named by the failure it raises.
     */
    private final @NotNull String expectation;

    /**
     * Predicate body that must yield {@code true} for the value to pass.
     */
    private final @NotNull Chain<T, Boolean> body;

    /**
     * Constructs an expect stage.
     *
     * @param inputType the type of the value flowing through this stage
     * @param expectation the sentence stating what the body checks, such as
     *                    {@code every row carries an id}
     * @param body the predicate sub-pipeline, consuming {@code T} and producing {@code BOOLEAN}
     * @return the stage
     * @param <T> value type
     * @throws IllegalArgumentException when {@code expectation} is blank or {@code body} fails
     *         type-chain validation
     */
    public static <T> @NotNull ExpectTransform<T> of(
        @Configurable(label = "Input type", placeholder = "STRING")
        @NotNull DataType<T> inputType,
        @Configurable(label = "Expectation", placeholder = "the id starts with item_")
        @NotNull String expectation,
        @Configurable(label = "Predicate body")
        @NotNull List<? extends Stage<?, ?>> body
    ) {
        if (expectation.isBlank())
            throw new IllegalArgumentException("ExpectTransform needs a non-blank expectation");

        ValidationReport report = Chain.validate(inputType, body, DataTypes.BOOLEAN);

        if (!report.isValid())
            throw new IllegalArgumentException("Invalid ExpectTransform body: " + report.issues());

        return new ExpectTransform<>(inputType, expectation, Chain.of(body));
    }

    /**
     * {@inheritDoc}
     *
     * @throws ExpectationFailedException when the body yields {@code false} or {@code null}
     */
    @Override
    public @Nullable T execute(@NotNull PipelineContext ctx, @Nullable T input) {
        if (input == null) return null;
        Boolean verdict = this.body.execute(ctx, input);

        if (Boolean.TRUE.equals(verdict))
            return input;

        throw new ExpectationFailedException(
            "Expectation '%s' failed: body yielded '%s' for input '%s' of type '%s'",
            this.expectation, verdict, preview(input), this.inputType.label()
        );
    }

    /**
     * Renders {@code value} for a failure message, cut after its first {@code PREVIEW_LENGTH}
     * code points so the cut never splits a surrogate pair.
     *
     * @param value the failing input
     * @return the rendered value, followed by {@code ...} when it was cut
     */
    private static @NotNull String preview(@NotNull Object value) {
        String text = String.valueOf(value);

        if (text.codePointCount(0, text.length()) <= PREVIEW_LENGTH)
            return text;

        return text.substring(0, text.offsetByCodePoints(0, PREVIEW_LENGTH)) + "...";
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<T> outputType() {
        return this.inputType;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull String summary() {
        return "Expect '" + this.expectation + "'";
    }

}
