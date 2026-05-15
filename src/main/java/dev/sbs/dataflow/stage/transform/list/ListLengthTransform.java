package dev.sbs.dataflow.stage.transform.list;

import dev.sbs.dataflow.DataType;
import dev.sbs.dataflow.DataTypes;
import dev.sbs.dataflow.PipelineContext;
import dev.sbs.dataflow.stage.StageConfig;
import dev.sbs.dataflow.stage.StageKind;
import dev.sbs.dataflow.stage.TransformStage;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.List;

/**
 * {@link TransformStage} that returns the size of an input list.
 *
 * @param <T> element type of the list
 */
@Getter
@Accessors(fluent = true)
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class ListLengthTransform<T> implements TransformStage<List<T>, Integer> {

    private final @NotNull DataType<T> elementType;

    private final @NotNull DataType<List<T>> listType;

    /**
     * Constructs a list-length stage for the given element type.
     *
     * @param elementType element type of the list
     * @return the stage
     * @param <T> element type
     */
    public static <T> @NotNull ListLengthTransform<T> of(@NotNull DataType<T> elementType) {
        return new ListLengthTransform<>(elementType, DataType.list(elementType));
    }

    /**
     * Reconstructs the transform from a populated {@link StageConfig}.
     *
     * @param cfg the populated configuration
     * @return the rebuilt stage
     */
    public static @NotNull ListLengthTransform<?> fromConfig(@NotNull StageConfig cfg) {
        return of(cfg.getDataType("elementType"));
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull StageConfig config() {
        return StageConfig.builder()
            .dataType("elementType", this.elementType)
            .build();
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable Integer execute(@NotNull PipelineContext ctx, @Nullable List<T> input) {
        return input == null ? null : input.size();
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<List<T>> inputType() {
        return this.listType;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull StageKind kind() {
        return StageKind.TRANSFORM_LIST_LENGTH;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<Integer> outputType() {
        return DataTypes.INT;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull String summary() {
        return "List length";
    }

}
