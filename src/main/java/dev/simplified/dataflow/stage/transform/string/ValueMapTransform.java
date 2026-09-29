package dev.simplified.dataflow.stage.transform.string;

import dev.simplified.annotations.AccessLevel;
import dev.simplified.annotations.Getter;
import dev.simplified.annotations.NamingStyle;
import dev.simplified.annotations.RequiredArgsConstructor;
import dev.simplified.collection.Concurrent;
import dev.simplified.dataflow.DataType;
import dev.simplified.dataflow.DataTypes;
import dev.simplified.dataflow.PipelineContext;
import dev.simplified.dataflow.stage.TransformStage;
import dev.simplified.dataflow.stage.meta.Configurable;
import dev.simplified.dataflow.stage.meta.StageSpec;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Map;

/**
 * {@link TransformStage} that maps a {@link String} through a fixed table.
 * <p>
 * A key is matched exactly and case-sensitively against the whole input, and a match returns
 * the key's value. An input the table lacks resolves one of three ways:
 * <ul>
 *   <li><b>default</b> - the {@code defaultValue} when one is set</li>
 *   <li><b>strict</b> - {@code null} when {@code strict} is {@code true}, so the element drops</li>
 *   <li><b>pass-through</b> - otherwise the input, unchanged</li>
 * </ul>
 * A value that stands for several values is written comma-joined and followed by
 * {@link SplitTransform}.
 */
@StageSpec(
    id = "TRANSFORM_VALUE_MAP",
    displayName = "Value map",
    description = "STRING -> STRING",
    category = StageSpec.Category.TRANSFORM_STRING
)
@Getter(style = NamingStyle.FLUENT)
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class ValueMapTransform implements TransformStage<String, String> {

    /**
     * Keys mapped to their values, in declared order.
     */
    private final @NotNull Map<String, String> table;

    /**
     * Value returned for an input the table lacks, or {@code null} when none is configured.
     */
    private final @Nullable String defaultValue;

    /**
     * Whether an input the table lacks rejects with {@code null}, or {@code null} when not
     * configured, which is false.
     */
    private final @Nullable Boolean strict;

    /**
     * Constructs a value-map stage.
     *
     * @param table the keys and the values they map to, copied in their iteration order
     * @param defaultValue the value for an input the table lacks, or {@code null} for none
     * @param strict whether an input the table lacks rejects with {@code null}, where {@code null} is false
     * @return the stage
     * @throws IllegalArgumentException when {@code table} holds a {@code null} key or value, or
     *         {@code defaultValue} is set and {@code strict} is true
     */
    public static @NotNull ValueMapTransform of(
        @Configurable(label = "Table", placeholder = "{\"PINK\":\"LIGHT_PURPLE\"}")
        @NotNull Map<String, String> table,
        @Configurable(label = "Default value (optional)", placeholder = "WHITE", optional = true)
        @Nullable String defaultValue,
        @Configurable(label = "Strict (optional)", placeholder = "false", optional = true)
        @Nullable Boolean strict
    ) {
        for (Map.Entry<String, String> entry : table.entrySet()) {
            if (entry.getKey() == null)
                throw new IllegalArgumentException("ValueMapTransform table holds a null key");

            if (entry.getValue() == null)
                throw new IllegalArgumentException(String.format("ValueMapTransform table maps '%s' to null", entry.getKey()));
        }

        if (defaultValue != null && Boolean.TRUE.equals(strict))
            throw new IllegalArgumentException("ValueMapTransform takes 'defaultValue' or 'strict', not both");

        return new ValueMapTransform(Concurrent.newUnmodifiableLinkedMap(table), defaultValue, strict);
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable String execute(@NotNull PipelineContext ctx, @Nullable String input) {
        if (input == null) return null;
        String mapped = this.table.get(input);
        if (mapped != null) return mapped;
        if (this.defaultValue != null) return this.defaultValue;
        return Boolean.TRUE.equals(this.strict) ? null : input;
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
        String miss;
        if (this.defaultValue != null)
            miss = "default '" + this.defaultValue + "'";
        else
            miss = Boolean.TRUE.equals(this.strict) ? "strict" : "pass-through";

        return "Value map (" + this.table.size() + " entr" + (this.table.size() == 1 ? "y" : "ies") + ", " + miss + ")";
    }

}
