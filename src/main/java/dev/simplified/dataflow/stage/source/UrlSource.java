package dev.simplified.dataflow.stage.source;

import dev.simplified.annotations.AccessLevel;
import dev.simplified.annotations.Getter;
import dev.simplified.annotations.NamingStyle;
import dev.simplified.annotations.RequiredArgsConstructor;
import dev.simplified.client.exception.UrlFetchException;
import dev.simplified.client.fetch.UrlFetcher;
import dev.simplified.dataflow.DataType;
import dev.simplified.dataflow.DataTypes;
import dev.simplified.dataflow.PipelineContext;
import dev.simplified.dataflow.stage.SourceStage;
import dev.simplified.dataflow.stage.meta.Configurable;
import dev.simplified.dataflow.stage.meta.StageSpec;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.net.URI;

/**
 * {@link SourceStage} that fetches a URL via {@link UrlFetcher} and emits the response body
 * tagged as one of the {@code RAW_*} types.
 * <p>
 * The body is held to {@code maxBodyBytes} when one is configured and to the fetcher's
 * configured cap otherwise. The fetch throws a {@link UrlFetchException}, failing the run, on
 * every error status - each {@code 4xx}, {@code 408} and {@code 429} among them, and each
 * {@code 5xx} - on a status code the client's {@code HttpStatus} has no constant for, on a
 * transport failure, a body past the cap, or a request the local rate limit refuses.
 * <p>
 * A fetched body passes through the context's {@link PipelineContext#fetchGuard() fetch guard}
 * before it is emitted, and a guard that throws fails the run.
 */
@StageSpec(
    id = "SOURCE_URL",
    displayName = "URL Source",
    description = "() -> RAW_*",
    category = StageSpec.Category.SOURCE
)
@Getter(style = NamingStyle.FLUENT)
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class UrlSource implements SourceStage<String> {

    private final @NotNull String url;

    private final @NotNull DataType<String> outputType;

    /**
     * Largest body, in bytes, the fetch accepts, or {@code null} to hold the body to the
     * fetcher's configured cap.
     */
    private final @Nullable Long maxBodyBytes;

    private static final @NotNull java.util.Set<DataType<?>> SUPPORTED_OUTPUT_TYPES = java.util.Set.of(
        DataTypes.STRING, DataTypes.RAW_HTML, DataTypes.RAW_XML, DataTypes.RAW_JSON
    );

    /**
     * Constructs a URL source whose fetched body is tagged as {@code outputType}. The
     * supported types are {@code RAW_HTML}, {@code RAW_XML}, {@code RAW_JSON}, and plain
     * {@code STRING}; structured types must come from a downstream parse transform.
     *
     * @param outputType how to tag the fetched body
     * @param url the URL to fetch
     * @param maxBodyBytes the largest body, in bytes, the fetch accepts, or {@code null} for the
     *                     fetcher's configured cap
     * @return a new source
     * @throws IllegalArgumentException when {@code outputType} is not one of the supported types,
     *         or {@code maxBodyBytes} is negative
     */
    public static @NotNull UrlSource of(
        @Configurable(label = "Output type (RAW_HTML / RAW_XML / RAW_JSON / STRING)", placeholder = "RAW_HTML")
        @NotNull DataType<String> outputType,
        @Configurable(label = "URL", placeholder = "https://example.com/page")
        @NotNull String url,
        @Configurable(label = "Body cap in bytes (optional)", placeholder = "10485760", optional = true)
        @Nullable Long maxBodyBytes
    ) {
        if (!SUPPORTED_OUTPUT_TYPES.contains(outputType))
            throw new IllegalArgumentException(
                "UrlSource supports " + SUPPORTED_OUTPUT_TYPES + " but got " + outputType
            );
        if (maxBodyBytes != null && maxBodyBytes < 0)
            throw new IllegalArgumentException("UrlSource maxBodyBytes must not be negative but got '" + maxBodyBytes + "'");
        return new UrlSource(url, outputType, maxBodyBytes);
    }

    /**
     * Constructs a URL source held to the fetcher's configured body cap. Equivalent to
     * {@link #of(DataType, String, Long) of(outputType, url, null)}.
     *
     * @param outputType how to tag the fetched body
     * @param url the URL to fetch
     * @return a new source
     * @throws IllegalArgumentException when {@code outputType} is not one of the supported types
     */
    public static @NotNull UrlSource of(@NotNull DataType<String> outputType, @NotNull String url) {
        return of(outputType, url, null);
    }

    /**
     * Convenience factory for an HTML body. Equivalent to
     * {@link #of(DataType, String) of(DataTypes.RAW_HTML, url)}.
     *
     * @param url the URL to fetch
     * @return a new source
     */
    public static @NotNull UrlSource rawHtml(@NotNull String url) {
        return of(DataTypes.RAW_HTML, url);
    }

    /**
     * Convenience factory for an XML body. Equivalent to
     * {@link #of(DataType, String) of(DataTypes.RAW_XML, url)}.
     *
     * @param url the URL to fetch
     * @return a new source
     */
    public static @NotNull UrlSource rawXml(@NotNull String url) {
        return of(DataTypes.RAW_XML, url);
    }

    /**
     * Convenience factory for a JSON body. Equivalent to
     * {@link #of(DataType, String) of(DataTypes.RAW_JSON, url)}.
     *
     * @param url the URL to fetch
     * @return a new source
     */
    public static @NotNull UrlSource rawJson(@NotNull String url) {
        return of(DataTypes.RAW_JSON, url);
    }

    /**
     * Convenience factory for a plain-{@link String} body. Equivalent to
     * {@link #of(DataType, String) of(DataTypes.STRING, url)}.
     *
     * @param url the URL to fetch
     * @return a new source
     */
    public static @NotNull UrlSource text(@NotNull String url) {
        return of(DataTypes.STRING, url);
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable String execute(@NotNull PipelineContext ctx, @Nullable Void input) {
        URI uri = URI.create(this.url);
        String body = this.maxBodyBytes == null
            ? ctx.fetcher().get(uri).getBody()
            : ctx.fetcher().get(uri, this.maxBodyBytes).getBody();
        ctx.fetchGuard().check(uri, body);
        return body;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull String summary() {
        String cap = this.maxBodyBytes == null ? "" : " (cap " + this.maxBodyBytes + " bytes)";
        return "URL " + this.outputType.label() + " " + this.url + cap;
    }

}
