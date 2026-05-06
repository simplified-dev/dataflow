package dev.sbs.dataflow.stage;

import dev.sbs.dataflow.DataPipeline;

import java.util.List;

/**
 * Terminal {@link Stage} that reduces a list-shaped input into a final value, normally the
 * last stage of a {@link DataPipeline}.
 *
 * @param <I> input type, normally a {@link List}
 * @param <O> output type
 */
public non-sealed interface CollectStage<I, O> extends Stage<I, O> {
}
