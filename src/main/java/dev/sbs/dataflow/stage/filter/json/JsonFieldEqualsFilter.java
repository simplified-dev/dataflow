package dev.sbs.dataflow.stage.filter.json;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import dev.sbs.dataflow.DataType;
import dev.sbs.dataflow.DataTypes;
import dev.sbs.dataflow.PipelineContext;
import dev.sbs.dataflow.stage.Configurable;
import dev.sbs.dataflow.stage.FilterStage;
import dev.sbs.dataflow.stage.StageSpec;
import dev.simplified.collection.Concurrent;
import dev.simplified.collection.ConcurrentList;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.List;

/**
 * {@link FilterStage} keeping only {@link JsonObject}s whose named field is a primitive
 * equal to the configured string value.
 */
@StageSpec(
    id = "FILTER_JSON_FIELD_EQUALS",
    displayName = "Field equals",
    description = "List<JSON_OBJECT> -> List<JSON_OBJECT>",
    category = StageSpec.Category.FILTER_JSON
)
@Getter
@Accessors(fluent = true)
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
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
    public static @NotNull JsonFieldEqualsFilter of(
        @Configurable(label = "Field name", placeholder = "rare")
        @NotNull String fieldName,
        @Configurable(label = "Equals", placeholder = "true")
        @NotNull String expectedValue
    ) {
        return new JsonFieldEqualsFilter(fieldName, expectedValue);
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable ConcurrentList<JsonObject> execute(@NotNull PipelineContext ctx, @Nullable List<JsonObject> input) {
        if (input == null) return null;
        return input.stream()
            .filter(this::matches)
            .collect(Concurrent.toUnmodifiableList());
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<List<JsonObject>> inputType() {
        return LIST_OBJ;
    }
    private boolean matches(@NotNull JsonObject o) {
        if (!o.has(this.fieldName)) return false;
        JsonElement v = o.get(this.fieldName);
        return v.isJsonPrimitive() && this.expectedValue.equals(v.getAsString());
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<List<JsonObject>> outputType() {
        return LIST_OBJ;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull String summary() {
        return "Field " + this.fieldName + "='" + this.expectedValue + "'";
    }

}
