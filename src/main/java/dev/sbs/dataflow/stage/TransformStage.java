package dev.sbs.dataflow.stage;

/**
 * {@link Stage} that maps a single input value to a single output value, possibly of a
 * different {@link dev.sbs.dataflow.DataType}.
 *
 * @param <I> input type
 * @param <O> output type
 */
public non-sealed interface TransformStage<I, O> extends Stage<I, O> {
}
