package dev.sbs.dataflow.stage.transform.list;

import dev.sbs.dataflow.DataType;
import dev.sbs.dataflow.PipelineContext;
import dev.sbs.dataflow.stage.StageId;
import dev.sbs.dataflow.stage.TransformStage;
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
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
@Getter
@Accessors(fluent = true)
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

    /** {@inheritDoc} */
    @Override public @NotNull DataType<List<T>> inputType()  { return this.listType; }
    /** {@inheritDoc} */
    @Override public @NotNull DataType<List<T>> outputType() { return this.listType; }
    /** {@inheritDoc} */
    @Override public @NotNull StageId kind()                 { return StageId.TRANSFORM_REVERSE; }
    /** {@inheritDoc} */
    @Override public @NotNull String summary()               { return "Reverse " + this.elementType.label(); }

    /** {@inheritDoc} */
    @Override
    public @Nullable List<T> execute(@NotNull PipelineContext ctx, @Nullable List<T> input) {
        if (input == null) return null;
        List<T> copy = new ArrayList<>(input);
        Collections.reverse(copy);
        return List.copyOf(copy);
    }

}
