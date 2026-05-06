package dev.sbs.dataflow.stage;

import java.util.List;

/**
 * {@link Stage} that subsets a {@link List} value, returning the same list type with a
 * possibly-smaller element population.
 *
 * @param <T> element type of the list
 */
public non-sealed interface FilterStage<T> extends Stage<List<T>, List<T>> {
}
