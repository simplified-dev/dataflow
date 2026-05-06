package dev.sbs.dataflow.stage.filter;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import dev.sbs.dataflow.DataType;
import dev.sbs.dataflow.DataTypes;
import dev.sbs.dataflow.PipelineContext;
import dev.sbs.dataflow.stage.FilterStage;
import dev.sbs.dataflow.stage.StageId;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.List;
import java.util.stream.Collectors;

/**
 * {@link FilterStage} keeping only {@link JsonObject}s whose named field is a primitive
 * equal to the configured string value.
 */
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
@Getter
@Accessors(fluent = true)
public final class JsonFieldEqualsFilter implements FilterStage<JsonObject> {

    private static final @NotNull DataType<List<JsonObject>> LIST_OBJ = DataType.list(DataTypes.JSON_OBJECT);

    private final @NotNull String fieldName;
    private final @NotNull String expectedValue;

    /**
     * Constructs a field-equals filter that compares the configured field's primitive
     * value to {@code expectedValue} (string-wise).
     *
     * @param fieldName the JSON field name
     * @param expectedValue the required primitive value (string compared via getAsString)
     * @return the stage
     */
    public static @NotNull JsonFieldEqualsFilter of(@NotNull String fieldName, @NotNull String expectedValue) {
        return new JsonFieldEqualsFilter(fieldName, expectedValue);
    }

    /** {@inheritDoc} */ @Override public @NotNull DataType<List<JsonObject>> inputType()  { return LIST_OBJ; }
    /** {@inheritDoc} */ @Override public @NotNull DataType<List<JsonObject>> outputType() { return LIST_OBJ; }
    /** {@inheritDoc} */ @Override public @NotNull StageId kind()                          { return StageId.FILTER_JSON_FIELD_EQUALS; }
    /** {@inheritDoc} */ @Override public @NotNull String summary()                        { return "Field " + this.fieldName + "='" + this.expectedValue + "'"; }

    /** {@inheritDoc} */
    @Override
    public @Nullable List<JsonObject> execute(@NotNull PipelineContext ctx, @Nullable List<JsonObject> input) {
        if (input == null) return null;
        return input.stream().filter(this::matches).collect(Collectors.toUnmodifiableList());
    }

    private boolean matches(@NotNull JsonObject o) {
        if (!o.has(this.fieldName)) return false;
        JsonElement v = o.get(this.fieldName);
        return v.isJsonPrimitive() && this.expectedValue.equals(v.getAsString());
    }

}
