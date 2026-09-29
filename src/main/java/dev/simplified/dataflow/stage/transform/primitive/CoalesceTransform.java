package dev.simplified.dataflow.stage.transform.primitive;

import dev.simplified.annotations.AccessLevel;
import dev.simplified.annotations.Getter;
import dev.simplified.annotations.NamingStyle;
import dev.simplified.annotations.RequiredArgsConstructor;
import dev.simplified.dataflow.DataType;
import dev.simplified.dataflow.DataTypes;
import dev.simplified.dataflow.PipelineContext;
import dev.simplified.dataflow.ValidationReport;
import dev.simplified.dataflow.chain.Chain;
import dev.simplified.dataflow.stage.Stage;
import dev.simplified.dataflow.stage.TransformStage;
import dev.simplified.dataflow.stage.meta.Configurable;
import dev.simplified.dataflow.stage.meta.StageSpec;
import dev.simplified.dataflow.stage.source.LiteralSource;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.List;

/**
 * {@link TransformStage} that yields the first non-{@code null} of a body, a fallback body and a
 * default value, each run against the same input.
 * <p>
 * A {@code null} input stays {@code null}, so a missing element stays missing; the stage fills a
 * missing value of a present element. When the optional {@code when} body is configured and
 * yields anything but {@code true}, the body is skipped and the fallback and default are tried.
 * The fallback runs only when the body is skipped or yields {@code null}, and the default is
 * returned only when the fallback is absent or yields {@code null} too. A stage built with
 * neither a fallback nor a default is refused, since it would only repeat its body. Three
 * alternatives nest a second coalesce as the fallback.
 * <p>
 * The default is parsed from its configured string once, when the stage is built, under the
 * types {@link LiteralSource} admits, as {@link ConstantTransform} parses its constant.
 *
 * @param <I> input type
 * @param <O> output type
 */
@StageSpec(
    id = "TRANSFORM_COALESCE",
    displayName = "Coalesce",
    description = "I -> O (body, fallback: I -> O; when: I -> BOOLEAN)",
    category = StageSpec.Category.TRANSFORM_PRIMITIVE
)
@Getter(style = NamingStyle.FLUENT)
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class CoalesceTransform<I, O> implements TransformStage<I, O> {

    private final @NotNull DataType<I> inputType;

    private final @NotNull DataType<O> outputType;

    private final @NotNull Chain<I, O> body;

    /**
     * Body tried when {@link #body} is skipped or yields {@code null}, or {@code null} when none
     * is configured.
     */
    private final @Nullable Chain<I, O> fallback;

    /**
     * Default exactly as configured, carried on the wire under {@code defaultValue}, or
     * {@code null} when none is configured.
     */
    private final @Nullable String defaultValue;

    /**
     * Default parsed under {@link #outputType}, returned when every body yields {@code null}, or
     * {@code null} when no default is configured.
     */
    private final @Nullable O parsedDefault;

    /**
     * Condition that must yield {@code true} for {@link #body} to run, or {@code null} when the
     * body always runs.
     */
    private final @Nullable Chain<I, Boolean> when;

    /**
     * Constructs a coalesce stage.
     *
     * @param inputType the input type every body consumes
     * @param outputType the output type the body and the fallback produce
     * @param body the first alternative, consuming {@code I} and producing {@code O}
     * @param fallback the second alternative, consuming {@code I} and producing {@code O}, or
     *                 {@code null} for none
     * @param defaultValue the serialized form of the last alternative, parsed under
     *                     {@code outputType}, or {@code null} for none
     * @param when the condition gating {@code body}, consuming {@code I} and producing
     *             {@code BOOLEAN}, or {@code null} to run the body for every input
     * @return the stage
     * @param <I> input type
     * @param <O> output type
     * @throws IllegalArgumentException when a body fails type-chain validation, when both
     *         {@code fallback} and {@code defaultValue} are absent, or when {@code defaultValue}
     *         does not parse as {@code outputType}
     */
    @SuppressWarnings("unchecked")
    public static <I, O> @NotNull CoalesceTransform<I, O> of(
        @Configurable(label = "Input type", placeholder = "STRING")
        @NotNull DataType<I> inputType,
        @Configurable(label = "Output type", placeholder = "STRING")
        @NotNull DataType<O> outputType,
        @Configurable(label = "Body")
        @NotNull List<? extends Stage<?, ?>> body,
        @Configurable(label = "Fallback body (optional)", optional = true)
        @Nullable List<? extends Stage<?, ?>> fallback,
        @Configurable(label = "Default value (optional)", placeholder = "OTHER", optional = true)
        @Nullable String defaultValue,
        @Configurable(label = "Condition body (optional)", optional = true)
        @Nullable List<? extends Stage<?, ?>> when
    ) {
        requireValid("body", Chain.validate(inputType, body, outputType));

        if (fallback != null)
            requireValid("fallback", Chain.validate(inputType, fallback, outputType));

        if (when != null)
            requireValid("when", Chain.validate(inputType, when, DataTypes.BOOLEAN));

        if (fallback == null && defaultValue == null)
            throw new IllegalArgumentException("CoalesceTransform needs a fallback or a defaultValue");

        O parsedDefault = defaultValue == null
            ? null
            : (O) ConstantTransform.parse("CoalesceTransform", "defaultValue", outputType, defaultValue);

        return new CoalesceTransform<>(
            inputType,
            outputType,
            Chain.of(body),
            fallback == null ? null : Chain.of(fallback),
            defaultValue,
            parsedDefault,
            when == null ? null : Chain.of(when)
        );
    }

    private static void requireValid(@NotNull String slot, @NotNull ValidationReport report) {
        if (!report.isValid())
            throw new IllegalArgumentException("Invalid CoalesceTransform " + slot + ": " + report.issues());
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable O execute(@NotNull PipelineContext ctx, @Nullable I input) {
        if (input == null) return null;

        if (this.when == null || Boolean.TRUE.equals(this.when.execute(ctx, input))) {
            O primary = this.body.execute(ctx, input);
            if (primary != null) return primary;
        }

        if (this.fallback != null) {
            O alternative = this.fallback.execute(ctx, input);
            if (alternative != null) return alternative;
        }

        return this.parsedDefault;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull String summary() {
        StringBuilder summary = new StringBuilder("Coalesce ")
            .append(this.inputType.label())
            .append(" -> ")
            .append(this.outputType.label());

        if (this.when != null) summary.append(", when");
        if (this.fallback != null) summary.append(", fallback");
        if (this.defaultValue != null) summary.append(", default '").append(this.defaultValue).append('\'');

        return summary.toString();
    }

}
