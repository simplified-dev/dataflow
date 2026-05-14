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
 * {@link TransformStage} that returns {@code true} when the input element's
 * {@link Element#text()} contains the configured substring.
 */
@Getter
@Accessors(fluent = true)
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class DomTextContainsPredicate implements TransformStage<Element, Boolean> {

    private final @NotNull String needle;

    /**
     * Constructs a node-text-contains predicate.
     *
     * @param needle the substring to look for in the element's text
     * @return the stage
     */
    public static @NotNull DomTextContainsPredicate of(@NotNull String needle) {
        return new DomTextContainsPredicate(needle);
    }

    /**
     * Reconstructs the predicate from a populated {@link StageConfig}.
     *
     * @param cfg the populated configuration
     * @return the rebuilt stage
     */
    public static @NotNull DomTextContainsPredicate fromConfig(@NotNull StageConfig cfg) {
        return of(cfg.getString("needle"));
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull StageConfig config() {
        return StageConfig.builder()
            .string("needle", this.needle)
            .build();
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable Boolean execute(@NotNull PipelineContext ctx, @Nullable Element input) {
        return input != null && input.text().contains(this.needle);
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<Element> inputType() {
        return DataTypes.DOM_NODE;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull StageKind kind() {
        return StageKind.PREDICATE_DOM_TEXT_CONTAINS;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<Boolean> outputType() {
        return DataTypes.BOOLEAN;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull String summary() {
        return "Text contains '" + this.needle + "'";
    }

}
