package dev.simplified.dataflow.stage;

import dev.simplified.dataflow.DataPipeline;
import dev.simplified.dataflow.DataType;
import dev.simplified.dataflow.DataTypes;
import org.jetbrains.annotations.NotNull;

/**
 * {@link Stage} that produces a value with no upstream input. Always the first stage of a
 * {@link DataPipeline}.
 *
 * @param <O> output type
 */
public non-sealed interface SourceStage<O> extends Stage<Void, O> {

    /** {@inheritDoc} */
    @Override
    default @NotNull DataType<Void> inputType() {
        return DataTypes.NONE;
    }

}
