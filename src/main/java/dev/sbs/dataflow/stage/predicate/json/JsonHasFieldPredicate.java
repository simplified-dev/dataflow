package dev.sbs.dataflow.stage.predicate.json;

import com.google.gson.JsonObject;
import dev.sbs.dataflow.DataType;
import dev.sbs.dataflow.DataTypes;
import dev.sbs.dataflow.PipelineContext;
import dev.sbs.dataflow.stage.meta.Configurable;
import dev.sbs.dataflow.stage.meta.StageSpec;
import dev.sbs.dataflow.stage.TransformStage;
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
public final class JsonHasFieldPredicate implements TransformStage<JsonObject, Boolean> {

    private final @NotNull String fieldName;

    /**
     * Constructs a has-field predicate.
     *
     * @param fieldName the JSON field name that must be present
     * @return the stage
     */
    public static @NotNull JsonHasFieldPredicate of(
        @Configurable(label = "Field name", placeholder = "rare")
        @NotNull String fieldName
    ) {
        return new JsonHasFieldPredicate(fieldName);
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
