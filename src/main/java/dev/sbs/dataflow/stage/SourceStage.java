package dev.sbs.dataflow.stage;

import dev.sbs.dataflow.DataType;
import dev.sbs.dataflow.DataTypes;
import org.jetbrains.annotations.NotNull;

/**
 * {@link Stage} that produces a value with no upstream input. Always the first stage of a
 * {@link dev.sbs.dataflow.DataPipeline}.
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
