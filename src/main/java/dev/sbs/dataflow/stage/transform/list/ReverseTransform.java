package dev.sbs.dataflow.stage.transform.list;

import dev.sbs.dataflow.DataType;
import dev.sbs.dataflow.PipelineContext;
import dev.sbs.dataflow.stage.StageConfig;
import dev.sbs.dataflow.stage.StageKind;
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
import java.util.Collections;
import java.util.List;

/**
 * {@link TransformStage} that returns a new list containing the input elements in reverse
 * order.
 *
 * @param <T> element type
 */
@Getter
@Accessors(fluent = true)
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class ReverseTransform<T> implements TransformStage<List<T>, List<T>> {

    private final @NotNull DataType<T> elementType;

    private final @NotNull DataType<List<T>> listType;

    /**
     * Constructs a reverse stage for the given element type.
     *
     * @param elementType element type of the list
     * @return the stage
     * @param <T> element type
     */
    public static <T> @NotNull ReverseTransform<T> of(@NotNull DataType<T> elementType) {
        return new ReverseTransform<>(elementType, DataType.list(elementType));
    }

    /**
     * Reconstructs the transform from a populated {@link StageConfig}.
     *
     * @param cfg the populated configuration
     * @return the rebuilt stage
     */
    public static @NotNull ReverseTransform<?> fromConfig(@NotNull StageConfig cfg) {
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
    public @Nullable ConcurrentList<T> execute(@NotNull PipelineContext ctx, @Nullable List<T> input) {
        if (input == null) return null;
        List<T> copy = new ArrayList<>(input);
        Collections.reverse(copy);
        return Concurrent.newUnmodifiableList(copy);
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<List<T>> inputType() {
        return this.listType;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull StageKind kind() {
        return StageKind.TRANSFORM_REVERSE;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<List<T>> outputType() {
        return this.listType;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull String summary() {
        return "Reverse " + this.elementType.label();
    }

}
