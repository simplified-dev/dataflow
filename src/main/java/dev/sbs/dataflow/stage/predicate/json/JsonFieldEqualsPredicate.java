package dev.sbs.dataflow.stage.predicate.json;

import com.google.gson.JsonElement;
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
 * {@link TransformStage} that returns {@code true} when the input {@link JsonObject}'s named
 * field is a primitive equal to the configured string value.
 */
@StageSpec(
    id = "PREDICATE_JSON_FIELD_EQUALS",
    displayName = "Field equals",
    description = "JSON_OBJECT -> BOOLEAN",
    category = StageSpec.Category.PREDICATE_JSON
)
@Getter
@Accessors(fluent = true)
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class JsonFieldEqualsPredicate implements TransformStage<JsonObject, Boolean> {

    private final @NotNull String fieldName;

    private final @NotNull String expectedValue;

    /**
     * Constructs a field-equals predicate.
     *
     * @param fieldName the JSON field name
     * @param expectedValue the required primitive value (compared via {@code getAsString})
     * @return the stage
     */
    public static @NotNull JsonFieldEqualsPredicate of(
        @Configurable(label = "Field name", placeholder = "rare")
        @NotNull String fieldName,
        @Configurable(label = "Equals", placeholder = "true")
        @NotNull String expectedValue
    ) {
        return new JsonFieldEqualsPredicate(fieldName, expectedValue);
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable Boolean execute(@NotNull PipelineContext ctx, @Nullable JsonObject input) {
        if (input == null || !input.has(this.fieldName)) return false;
        JsonElement v = input.get(this.fieldName);
        return v.isJsonPrimitive() && this.expectedValue.equals(v.getAsString());
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
        return "Field " + this.fieldName + "='" + this.expectedValue + "'";
    }

}
