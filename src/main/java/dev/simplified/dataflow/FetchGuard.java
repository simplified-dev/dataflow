package dev.simplified.dataflow;

import dev.simplified.dataflow.stage.source.UrlSource;
import dev.simplified.dataflow.stage.transform.string.FetchTransform;
import org.jetbrains.annotations.NotNull;

import java.net.URI;

/**
 * Check a host runs over every body a stage fetches, before the stage hands the body on.
 * <p>
 * The host installs one on a {@link PipelineContext}, and every {@link UrlSource} and
 * {@link FetchTransform} run against that context passes each body it fetches through it - a
 * body the response cache replays included - so a check specific to a source, such as refusing
 * an error page an origin answers with a success status, is written once rather than in every
 * pipeline that reads the source.
 * <p>
 * A guard refuses a body by throwing, and the throw fails the run. {@link FetchTransform} never
 * turns a refusal into a dropped element, whatever the exception's type.
 */
@FunctionalInterface
public interface FetchGuard {

    /**
     * Guard that accepts every body. A {@link PipelineContext} built without a guard carries it.
     */
    @NotNull FetchGuard NOOP = (uri, body) -> { };

    /**
     * Checks a body a stage fetched, throwing to refuse it.
     *
     * @param uri the URL the body was fetched from
     * @param body the fetched body
     */
    void check(@NotNull URI uri, @NotNull String body);

}
