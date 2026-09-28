package dev.simplified.dataflow.stage.fixture;

import dev.simplified.annotations.AccessLevel;
import dev.simplified.annotations.Getter;
import dev.simplified.annotations.NamingStyle;
import dev.simplified.annotations.RequiredArgsConstructor;
import dev.simplified.collection.Concurrent;
import dev.simplified.dataflow.DataType;
import dev.simplified.dataflow.DataTypes;
import dev.simplified.dataflow.PipelineContext;
import dev.simplified.dataflow.stage.FieldSpec;
import dev.simplified.dataflow.stage.TransformStage;
import dev.simplified.dataflow.stage.meta.Configurable;
import dev.simplified.dataflow.stage.meta.StageSpec;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Map;

/**
 * Test-only {@link TransformStage} that maps its input through a fixed table, exercising the
 * {@link FieldSpec.Type#STRING_MAP} slot from factory to wire.
 * <p>
 * Null in, null out; a key the table lacks passes through unchanged.
 */
@StageSpec(
    id = "TEST_LOOKUP_TABLE",
    displayName = "Lookup table (test fixture)",
    description = "STRING -> STRING",
    category = StageSpec.Category.TRANSFORM_STRING
)
@Getter(style = NamingStyle.FLUENT)
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class LookupTableTransform implements TransformStage<String, String> {

    private final @NotNull Map<String, String> table;

    /**
     * Constructs a stage mapping every input through {@code table}.
     *
     * @param table the lookup table, copied in its iteration order
     * @return the stage
     */
    public static @NotNull LookupTableTransform of(
        @Configurable(label = "Table", placeholder = "{\"a\":\"b\"}")
        @NotNull Map<String, String> table
    ) {
        return new LookupTableTransform(Concurrent.newUnmodifiableLinkedMap(table));
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable String execute(@NotNull PipelineContext ctx, @Nullable String input) {
        if (input == null) return null;
        return this.table.getOrDefault(input, input);
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
        return "Lookup table (" + this.table.size() + " entries)";
    }

}
