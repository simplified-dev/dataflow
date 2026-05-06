package dev.sbs.dataflow.stage.filter.json;

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

/** {@link FilterStage} keeping only {@link JsonObject}s that contain the named field. */
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
@Getter
@Accessors(fluent = true)
public final class JsonHasFieldFilter implements FilterStage<JsonObject> {

    private static final @NotNull DataType<List<JsonObject>> LIST_OBJ = DataType.list(DataTypes.JSON_OBJECT);
    private final @NotNull String fieldName;

    /**
     * Constructs a has-field filter.
     *
     * @param fieldName the JSON field name that must be present
     * @return the stage
     */
    public static @NotNull JsonHasFieldFilter of(@NotNull String fieldName) {
        return new JsonHasFieldFilter(fieldName);
    }

    /** {@inheritDoc} */ @Override public @NotNull DataType<List<JsonObject>> inputType()  { return LIST_OBJ; }
    /** {@inheritDoc} */ @Override public @NotNull DataType<List<JsonObject>> outputType() { return LIST_OBJ; }
    /** {@inheritDoc} */ @Override public @NotNull StageId kind()                          { return StageId.FILTER_JSON_HAS_FIELD; }
    /** {@inheritDoc} */ @Override public @NotNull String summary()                        { return "Has field '" + this.fieldName + "'"; }

    /** {@inheritDoc} */
    @Override
    public @Nullable List<JsonObject> execute(@NotNull PipelineContext ctx, @Nullable List<JsonObject> input) {
        return input == null ? null : input.stream().filter(o -> o.has(this.fieldName)).collect(Collectors.toUnmodifiableList());
    }

}
