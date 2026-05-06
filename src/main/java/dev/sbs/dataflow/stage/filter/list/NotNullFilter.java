package dev.sbs.dataflow.stage.filter.list;

import dev.sbs.dataflow.DataType;
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
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * {@link FilterStage} dropping {@code null} elements from the list.
 *
 * @param <T> element type
 */
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
@Getter
@Accessors(fluent = true)
public final class NotNullFilter<T> implements FilterStage<T> {

    private final @NotNull DataType<T> elementType;
    private final @NotNull DataType<List<T>> listType;

    /**
     * Constructs a not-null filter for the given element type.
     *
     * @param elementType element type of the list
     * @return the stage
     * @param <T> element type
     */
    public static <T> @NotNull NotNullFilter<T> of(@NotNull DataType<T> elementType) {
        return new NotNullFilter<>(elementType, DataType.list(elementType));
    }

    /** {@inheritDoc} */ @Override public @NotNull DataType<List<T>> inputType()  { return this.listType; }
    /** {@inheritDoc} */ @Override public @NotNull DataType<List<T>> outputType() { return this.listType; }
    /** {@inheritDoc} */ @Override public @NotNull StageId kind()                 { return StageId.FILTER_NOT_NULL; }
    /** {@inheritDoc} */ @Override public @NotNull String summary()               { return "Not null " + this.elementType.label(); }

    /** {@inheritDoc} */
    @Override
    public @Nullable List<T> execute(@NotNull PipelineContext ctx, @Nullable List<T> input) {
        return input == null ? null : input.stream().filter(Objects::nonNull).collect(Collectors.toUnmodifiableList());
    }

}
