package dev.sbs.dataflow.stage.predicate.dom;

import dev.sbs.dataflow.DataType;
import dev.sbs.dataflow.DataTypes;
import dev.sbs.dataflow.PipelineContext;
import dev.sbs.dataflow.stage.StageConfig;
import dev.sbs.dataflow.stage.StageKind;
import dev.sbs.dataflow.stage.TransformStage;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jsoup.nodes.Element;

/**
 * {@link TransformStage} that returns {@code true} when the input element has the configured
 * attribute. If {@link #expectedValue} is non-null, the attribute value must additionally match.
 */
@Getter
@Accessors(fluent = true)
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class DomHasAttrPredicate implements TransformStage<Element, Boolean> {

    private final @NotNull String attributeName;

    private final @Nullable String expectedValue;

    /**
     * Constructs a has-attribute predicate that ignores attribute value.
     *
     * @param attributeName the attribute that must be present
     * @return the stage
     */
    public static @NotNull DomHasAttrPredicate of(@NotNull String attributeName) {
        return new DomHasAttrPredicate(attributeName, null);
    }

    /**
     * Constructs a has-attribute predicate that additionally matches the attribute value.
     *
     * @param attributeName the attribute that must be present
     * @param expectedValue the required attribute value
     * @return the stage
     */
    public static @NotNull DomHasAttrPredicate of(@NotNull String attributeName, @NotNull String expectedValue) {
        return new DomHasAttrPredicate(attributeName, expectedValue);
    }

    /**
     * Reconstructs the predicate from a populated {@link StageConfig}.
     *
     * @param cfg the populated configuration
     * @return the rebuilt stage
     */
    public static @NotNull DomHasAttrPredicate fromConfig(@NotNull StageConfig cfg) {
        String name = cfg.getString("attributeName");
        String value = cfg.getString("expectedValue");
        return value.isEmpty() ? of(name) : of(name, value);
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull StageConfig config() {
        return StageConfig.builder()
            .string("attributeName", this.attributeName)
            .string("expectedValue", this.expectedValue == null ? "" : this.expectedValue)
            .build();
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable Boolean execute(@NotNull PipelineContext ctx, @Nullable Element input) {
        if (input == null) return false;
        if (!input.hasAttr(this.attributeName)) return false;
        return this.expectedValue == null || this.expectedValue.equals(input.attr(this.attributeName));
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<Element> inputType() {
        return DataTypes.DOM_NODE;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull StageKind kind() {
        return StageKind.PREDICATE_DOM_HAS_ATTR;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<Boolean> outputType() {
        return DataTypes.BOOLEAN;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull String summary() {
        return this.expectedValue == null
            ? "Has attr '" + this.attributeName + "'"
            : "Attr " + this.attributeName + "='" + this.expectedValue + "'";
    }

}
