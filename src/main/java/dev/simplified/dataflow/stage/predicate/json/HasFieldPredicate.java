package dev.simplified.dataflow.stage.predicate.json;

import com.google.gson.JsonObject;
import dev.simplified.dataflow.DataType;
import dev.simplified.dataflow.DataTypes;
import dev.simplified.dataflow.PipelineContext;
import dev.simplified.dataflow.stage.TransformStage;
import dev.simplified.dataflow.stage.meta.Configurable;
import dev.simplified.dataflow.stage.meta.StageSpec;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * {@link TransformStage} that returns {@code true} when the input {@link JsonObject} contains the named field.
 */
@StageSpec(
    id = "PREDICATE_JSON_HAS_FIELD",
    displayName = "Has field",
    description = "JSON_OBJECT -> BOOLEAN",
    category = StageSpec.Category.PREDICATE_JSON
)
@Getter
@Accessors(fluent = true)
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class HasFieldPredicate implements TransformStage<JsonObject, Boolean> {

    private final @NotNull String fieldName;

    /**
     * Constructs a has-field predicate.
     *
     * @param fieldName the JSON field name that must be present
     * @return the stage
     */
    public static @NotNull HasFieldPredicate of(
        @Configurable(label = "Field name", placeholder = "rare")
        @NotNull String fieldName
    ) {
        return new HasFieldPredicate(fieldName);
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable Boolean execute(@NotNull PipelineContext ctx, @Nullable JsonObject input) {
        return input != null && input.has(this.fieldName);
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<JsonObject> inputType() {
        return DataTypes.JSON_OBJECT;
    }
    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<Boolean> outputType() {
        return DataTypes.BOOLEAN;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull String summary() {
        return "Has field '" + this.fieldName + "'";
    }

}
