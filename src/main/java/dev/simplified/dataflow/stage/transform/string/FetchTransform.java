package dev.simplified.dataflow.stage.transform.string;

import dev.simplified.annotations.AccessLevel;
import dev.simplified.annotations.Getter;
import dev.simplified.annotations.NamingStyle;
import dev.simplified.annotations.RequiredArgsConstructor;
import dev.simplified.client.exception.UrlFetchException;
import dev.simplified.client.fetch.UrlFetcher;
import dev.simplified.client.response.HttpStatus;
import dev.simplified.dataflow.DataType;
import dev.simplified.dataflow.DataTypes;
import dev.simplified.dataflow.PipelineContext;
import dev.simplified.dataflow.stage.TransformStage;
import dev.simplified.dataflow.stage.meta.Configurable;
import dev.simplified.dataflow.stage.meta.StageSpec;
import dev.simplified.dataflow.stage.source.UrlSource;
import dev.simplified.dataflow.stage.transform.encoding.UrlEncodeTransform;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.net.URI;
import java.util.Set;

/**
 * {@link TransformStage} that fetches the URL its input names and emits the response body
 * tagged as {@code STRING} or one of the {@code RAW_*} types - {@link UrlSource} with the URL
 * taken from the running value, so a map body over page names reads one page per element.
 * <p>
 * The URL is the input itself, or {@code urlTemplate} with every {@code {}} replaced by the
 * input. The input is substituted as given, and the URL is sent in its
 * {@linkplain URI#toASCIIString() ASCII form}, so a character outside ASCII goes out as its UTF-8
 * bytes percent-encoded - an {@code e} with an acute accent as {@code %C3%A9}. A space does not
 * form a URI and a {@code ?} or {@code #} ends the path, so a page name placed in a path segment
 * has its spaces replaced first, by a {@link ReplaceTransform} of {@code " "} with {@code "_"}
 * for a MediaWiki title or with {@code "%20"} otherwise. {@link UrlEncodeTransform} writes a
 * space as {@code +}, which a path reads as a literal plus, so it encodes a query value and not a
 * path segment.
 * <p>
 * The fetch goes through {@link PipelineContext#fetcher()}, so it carries that
 * {@link UrlFetcher}'s headers, rate limit and response cache, and it is held to
 * {@code maxBodyBytes} when one is configured and to the fetcher's configured cap otherwise.
 * <p>
 * A {@code 2xx} body passes through the context's
 * {@link PipelineContext#fetchGuard() fetch guard} and is emitted - one under a {@code 2xx} code
 * the client's {@link HttpStatus} has no constant for among them, which the fetcher reads as a
 * {@code 200} and the response cache does not store. A redirect the fetcher follows is read
 * through to the page it leads to, and a {@code 304} answering the fetcher's own revalidation of a
 * cached copy is answered with the cached body. The guard is handed the URL and the body, not the
 * status.
 * <p>
 * A client error the origin answers - a {@link UrlFetchException.ClientError}, {@code 400} to
 * {@code 451} or a {@code 4xx} {@link HttpStatus} has no constant for outside Nginx's
 * {@code 494-499}, whatever the size of its body - rejects with {@code null}, so a map body drops
 * a page that does not exist, except a {@code 408} or a {@code 429}: a timeout or throttling says
 * nothing about whether the page exists, so it throws. Every other failure throws too - a
 * {@code 3xx} the fetcher does not follow, a {@code 5xx}, an Nginx {@code 444} or
 * {@code 494-499}, any other status outside the {@code 2xx} class, a transport failure, a
 * {@code 2xx} body past the cap, a request the local rate limit refuses, a guard that refuses the
 * body, a blank input, or an input that does not form a URI - so a collection is never silently
 * short a page because the server, the network or the body failed.
 * <p>
 * A {@code 3xx} the fetcher does not follow throws {@link UrlFetchException.Redirection}: a
 * {@code 300}, {@code 305} or {@code 306}, a {@code 304} that answers no revalidation the fetcher
 * made - one answering an {@code If-None-Match} or {@code If-Modified-Since} among the fetcher's
 * own headers included - a redirect with no {@code Location} header, a redirect to another host
 * or port from a fetcher that sends {@code Authorization} or {@code Cookie}, or a {@code 3xx} code
 * {@link HttpStatus} has no constant for.
 * <p>
 * A {@code 5xx} the origin answers while the fetcher refreshes a stale cached copy of the URL is
 * not raised when that copy's {@code stale-if-error} window is still open as the fetch begins and
 * no directive requires it revalidated: the fetcher answers the cached body in its place, and the
 * stage passes it through the guard and emits it like a fresh one, so a finished run can hold a
 * page an earlier fetch read.
 */
@StageSpec(
    id = "TRANSFORM_FETCH",
    displayName = "Fetch",
    description = "STRING -> RAW_*",
    category = StageSpec.Category.TRANSFORM_STRING
)
@Getter(style = NamingStyle.FLUENT)
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class FetchTransform implements TransformStage<String, String> {

    private static final @NotNull String INPUT_MARKER = "{}";

    private static final @NotNull Set<DataType<?>> SUPPORTED_OUTPUT_TYPES = Set.of(
        DataTypes.STRING, DataTypes.RAW_HTML, DataTypes.RAW_XML, DataTypes.RAW_JSON
    );

    /**
     * Client error codes that throw rather than reject - a timeout and throttling, neither of
     * which says the page is absent.
     */
    private static final @NotNull Set<Integer> THROWN_CLIENT_ERRORS = Set.of(
        HttpStatus.REQUEST_TIMEOUT.getCode(),
        HttpStatus.TOO_MANY_REQUESTS.getCode()
    );

    private final @NotNull DataType<String> outputType;

    /**
     * URL the input replaces every {@code {}} in, or {@code null} when the input is the URL.
     */
    private final @Nullable String urlTemplate;

    /**
     * Largest body, in bytes, a fetch accepts, or {@code null} to hold the body to the fetcher's
     * configured cap.
     */
    private final @Nullable Long maxBodyBytes;

    /**
     * Constructs a fetch stage.
     *
     * @param outputType how to tag the fetched body, one of {@code STRING}, {@code RAW_HTML},
     *                   {@code RAW_XML} and {@code RAW_JSON}
     * @param urlTemplate the URL the input replaces every {@code {}} in, or {@code null} to fetch
     *                    the input itself
     * @param maxBodyBytes the largest body, in bytes, a fetch accepts, or {@code null} for the
     *                     fetcher's configured cap
     * @return the stage
     * @throws IllegalArgumentException when {@code outputType} is not one of the supported types,
     *         {@code urlTemplate} holds no {@code {}}, or {@code maxBodyBytes} is negative
     */
    public static @NotNull FetchTransform of(
        @Configurable(label = "Output type (RAW_HTML / RAW_XML / RAW_JSON / STRING)", placeholder = "RAW_HTML")
        @NotNull DataType<String> outputType,
        @Configurable(label = "URL template, {} = the input (optional)", placeholder = "https://example.com/wiki/{}", optional = true)
        @Nullable String urlTemplate,
        @Configurable(label = "Body cap in bytes (optional)", placeholder = "10485760", optional = true)
        @Nullable Long maxBodyBytes
    ) {
        if (!SUPPORTED_OUTPUT_TYPES.contains(outputType)) {
            throw new IllegalArgumentException(
                "FetchTransform supports " + SUPPORTED_OUTPUT_TYPES + " but got '" + outputType.label() + "'"
            );
        }

        if (urlTemplate != null && !urlTemplate.contains(INPUT_MARKER)) {
            throw new IllegalArgumentException(
                "FetchTransform urlTemplate '" + urlTemplate + "' has no " + INPUT_MARKER + " for the input"
            );
        }

        if (maxBodyBytes != null && maxBodyBytes < 0)
            throw new IllegalArgumentException("FetchTransform maxBodyBytes must not be negative but got '" + maxBodyBytes + "'");

        return new FetchTransform(outputType, urlTemplate, maxBodyBytes);
    }

    /**
     * Constructs a fetch stage that fetches the input itself, held to the fetcher's configured
     * body cap. Equivalent to {@link #of(DataType, String, Long) of(outputType, null, null)}.
     *
     * @param outputType how to tag the fetched body
     * @return the stage
     * @throws IllegalArgumentException when {@code outputType} is not one of the supported types
     */
    public static @NotNull FetchTransform of(@NotNull DataType<String> outputType) {
        return of(outputType, null, null);
    }

    /**
     * Constructs a fetch stage that fetches {@code urlTemplate} with the input in place of every
     * {@code {}}, held to the fetcher's configured body cap. Equivalent to
     * {@link #of(DataType, String, Long) of(outputType, urlTemplate, null)}.
     *
     * @param outputType how to tag the fetched body
     * @param urlTemplate the URL the input replaces every {@code {}} in
     * @return the stage
     * @throws IllegalArgumentException when {@code outputType} is not one of the supported types,
     *         or {@code urlTemplate} holds no {@code {}}
     */
    public static @NotNull FetchTransform of(@NotNull DataType<String> outputType, @NotNull String urlTemplate) {
        return of(outputType, urlTemplate, null);
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable String execute(@NotNull PipelineContext ctx, @Nullable String input) {
        if (input == null) return null;

        if (input.isBlank())
            throw new IllegalArgumentException("FetchTransform input is blank, so it names no URL");

        String url = this.urlTemplate == null ? input : this.urlTemplate.replace(INPUT_MARKER, input);
        URI uri = URI.create(URI.create(url).toASCIIString());
        String body;

        try {
            body = this.maxBodyBytes == null
                ? ctx.fetcher().get(uri).getBody()
                : ctx.fetcher().get(uri, this.maxBodyBytes).getBody();
        } catch (UrlFetchException.ClientError ex) {
            if (THROWN_CLIENT_ERRORS.contains(ex.getStatusCode()))
                throw ex;

            return null;
        }

        ctx.fetchGuard().check(uri, body);
        return body;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull DataType<String> inputType() {
        return DataTypes.STRING;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull String summary() {
        String target = this.urlTemplate == null ? "<input>" : this.urlTemplate;
        String cap = this.maxBodyBytes == null ? "" : " (cap " + this.maxBodyBytes + " bytes)";
        return "Fetch " + this.outputType.label() + " " + target + cap;
    }

}
