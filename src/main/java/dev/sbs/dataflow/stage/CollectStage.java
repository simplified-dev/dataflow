package dev.sbs.dataflow.stage;

/**
 * Terminal {@link Stage} that reduces a list-shaped input into a final value, normally the
 * last stage of a {@link dev.sbs.dataflow.DataPipeline}.
 *
 * @param <I> input type, normally a {@link java.util.List}
 * @param <O> output type
 */
public non-sealed interface CollectStage<I, O> extends Stage<I, O> {
}
