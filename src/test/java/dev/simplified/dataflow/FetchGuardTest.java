package dev.simplified.dataflow;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.net.URI;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

/**
 * Covers the {@link FetchGuard} a {@link PipelineContext} carries when none is installed, and
 * the one it carries when one is.
 */
class FetchGuardTest {

    @Test
    @DisplayName("A defaulted context carries the no-op guard")
    void defaultsCarryNoop() {
        assertThat(PipelineContext.defaults().fetchGuard(), is(sameInstance(FetchGuard.NOOP)));
    }

    @Test
    @DisplayName("A context built without a guard carries the no-op guard")
    void builderDefaultIsNoop() {
        assertThat(PipelineContext.builder().build().fetchGuard(), is(sameInstance(FetchGuard.NOOP)));
    }

    @Test
    @DisplayName("The no-op guard accepts any body")
    void noopAcceptsEveryBody() {
        assertDoesNotThrow(() -> FetchGuard.NOOP.check(URI.create("https://example.com/page"), ""));
    }

    @Test
    @DisplayName("A context carries the guard its builder installs")
    void builderInstallsGuard() {
        FetchGuard guard = (uri, body) -> { };
        assertThat(PipelineContext.builder().withFetchGuard(guard).build().fetchGuard(), is(sameInstance(guard)));
    }

}
