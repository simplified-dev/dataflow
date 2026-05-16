package dev.simplified.dataflow.stage;

import dev.simplified.dataflow.DataType;

/**
 * {@link Stage} that maps a single input value to a single output value, possibly of a
 * different {@link DataType}.
 *
 * @param <I> input type
 * @param <O> output type
 */
public non-sealed interface TransformStage<I, O> extends Stage<I, O> {
}
