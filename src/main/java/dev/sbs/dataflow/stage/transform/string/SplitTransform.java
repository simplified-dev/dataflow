package dev.sbs.dataflow.stage.transform.string;

import dev.sbs.dataflow.DataType;
import dev.sbs.dataflow.DataTypes;
import dev.sbs.dataflow.PipelineContext;
import dev.sbs.dataflow.stage.meta.Configurable;
import dev.sbs.dataflow.stage.meta.StageSpec;
import dev.sbs.dataflow.stage.TransformStage;
import dev.simplified.collection.Concurrent;
import dev.simplified.collection.ConcurrentList;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.List;
import java.util.regex.Pattern;

/**
 * {@link TransformStage} that splits a {@link String} on a regex into a list of substrings,
 * equivalent to {@link String#split(String)}.
 */
@StageSpec(
    id = "TRANSFORM_SPLIT",
    displayName = "Split on regex",
    description = "STRING -> List<STRING>",
    category = StageSpec.Category.TRANSFORM_STRING
)
@Getter
@Accessors(fluent = true)
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class SplitTransform implements TransformStage<String, List<String>> {

    private static final @NotNull DataType<List<String>> OUTPUT = DataType.list(DataTypes.STRING);

    private final @NotNull String regex;

    private final @NotNull Pattern pattern;

    /**
     * Constructs a split stage.
     *
     * @param regex the regex used as a delimiter
     * @return the stage
     */
    public static @NotNull SplitTransform of(
        @Configurable(label = "Regex", placeholder = ",")
        @NotNull String regex
    ) {
        return new SplitTransform(regex, Pattern.compile(regex));
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable ConcurrentList<String> execute(@NotNull PipelineContext ctx, @Nullable String input) {
        if (input == null) return null;
        return Concurrent.newUnmodifiableList(this.pattern.split(input));
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<String> inputType() {
        return DataTypes.STRING;
    }
    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<List<String>> outputType() {
        return OUTPUT;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull String summary() {
        return "Split on '" + this.regex + "'";
    }

}
