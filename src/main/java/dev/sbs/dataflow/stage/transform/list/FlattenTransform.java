package dev.sbs.dataflow.stage.transform.list;

import dev.sbs.dataflow.DataType;
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

import java.util.ArrayList;
import java.util.List;

/**
 * {@link TransformStage} that concatenates a list of lists into a single list, preserving
 * order. Nested {@code null} sub-lists are skipped.
 *
 * @param <T> element type of the inner lists
 */
@StageSpec(
    id = "TRANSFORM_FLATTEN",
    displayName = "Flatten",
    description = "List<List<T>> -> List<T>",
    category = StageSpec.Category.TRANSFORM_LIST
)
@Getter
@Accessors(fluent = true)
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class FlattenTransform<T> implements TransformStage<List<List<T>>, List<T>> {

    private final @NotNull DataType<T> elementType;

    private final @NotNull DataType<List<T>> innerListType;

    private final @NotNull DataType<List<List<T>>> outerListType;

    /**
     * Constructs a flatten stage for the given element type.
     *
     * @param elementType element type of the inner lists
     * @return the stage
     * @param <T> element type
     */
    public static <T> @NotNull FlattenTransform<T> of(
        @Configurable(label = "Element type", placeholder = "STRING")
        @NotNull DataType<T> elementType
    ) {
        DataType<List<T>> inner = DataType.list(elementType);
        return new FlattenTransform<>(elementType, inner, DataType.list(inner));
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable ConcurrentList<T> execute(@NotNull PipelineContext ctx, @Nullable List<List<T>> input) {
        if (input == null) return null;
        List<T> result = new ArrayList<>();
        for (List<T> inner : input) {
            if (inner != null) result.addAll(inner);
        }
        return Concurrent.newUnmodifiableList(result);
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<List<List<T>>> inputType() {
        return this.outerListType;
    }
    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<List<T>> outputType() {
        return this.innerListType;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull String summary() {
        return "Flatten List<" + this.elementType.label() + ">";
    }

}
