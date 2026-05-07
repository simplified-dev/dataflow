package dev.sbs.dataflow.stage;

import dev.sbs.dataflow.DataType;
import dev.sbs.dataflow.DataTypes;
import org.jetbrains.annotations.NotNull;

import java.util.Map;

/**
 * Terminal {@link Stage} that fans an input value out to several named sub-pipelines and
 * collects their results into a {@link Map}.
 *
 * @param <I> input type, shared by every named sub-pipeline
 */
public non-sealed interface BranchStage<I> extends Stage<I, Map<String, Object>> {

    /** {@inheritDoc} */
    @Override
    default @NotNull StageKind kind() {
        return StageKind.BRANCH;
    }

    /** {@inheritDoc} */
    @Override
    default @NotNull DataType<Map<String, Object>> outputType() {
        return DataTypes.BRANCH_OUTPUT;
    }

}
