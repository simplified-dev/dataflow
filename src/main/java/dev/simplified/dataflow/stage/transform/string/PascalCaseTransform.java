package dev.simplified.dataflow.stage.transform.string;

import dev.simplified.dataflow.DataType;
import dev.simplified.dataflow.DataTypes;
import dev.simplified.dataflow.PipelineContext;
import dev.simplified.dataflow.stage.TransformStage;
import dev.simplified.dataflow.stage.meta.StageSpec;
import dev.simplified.util.StringUtil;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * {@link TransformStage} that converts an input {@link String} to {@code PascalCase}.
 * <p>
 * Word detection matches {@link CamelCaseTransform}: whitespace, {@code _}, {@code -},
 * lower-to-upper transitions, and acronym-to-capitalised-word transitions all split
 * words. Every word is lowercased then has its first character capitalised; separators
 * are dropped.
 * <p>
 * Examples:
 * <ul>
 *   <li>{@code "hello world"} -&gt; {@code "HelloWorld"}</li>
 *   <li>{@code "hello_world"} -&gt; {@code "HelloWorld"}</li>
 *   <li>{@code "helloWorld"} -&gt; {@code "HelloWorld"}</li>
 *   <li>{@code "XMLParser"} -&gt; {@code "XmlParser"}</li>
 * </ul>
 */
@StageSpec(
    id = "TRANSFORM_PASCAL_CASE",
    displayName = "Pascal case",
    description = "STRING -> STRING",
    category = StageSpec.Category.TRANSFORM_STRING
)
@Getter
@Accessors(fluent = true)
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class PascalCaseTransform implements TransformStage<String, String> {

    /**
     * Constructs a pascal-case stage.
     *
     * @return the stage
     */
    public static @NotNull PascalCaseTransform of() {
        return new PascalCaseTransform();
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable String execute(@NotNull PipelineContext ctx, @Nullable String input) {
        return input == null ? null : StringUtil.toPascalCase(input);
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<String> inputType() {
        return DataTypes.STRING;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<String> outputType() {
        return DataTypes.STRING;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull String summary() {
        return "PascalCase";
    }

}
