package dev.simplified.dataflow.stage.transform.string;

import dev.simplified.annotations.AccessLevel;
import dev.simplified.annotations.Getter;
import dev.simplified.annotations.NamingStyle;
import dev.simplified.annotations.RequiredArgsConstructor;
import dev.simplified.client.exception.UrlFetchException;
import dev.simplified.client.fetch.UrlFetcher;
import dev.simplified.dataflow.DataType;
import dev.simplified.dataflow.DataTypes;
import dev.simplified.dataflow.PipelineContext;
import dev.simplified.dataflow.stage.TransformStage;
import dev.simplified.dataflow.stage.meta.Configurable;
import dev.simplified.dataflow.stage.meta.StageSpec;
import dev.simplified.dataflow.stage.source.UrlSource;
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
 * input. The input is substituted as given, so a page name that needs escaping passes through an
 * encoding stage first. The fetch goes through {@link PipelineContext#fetcher()}, so it carries
 * that {@link UrlFetcher}'s headers, rate limit and response cache, and it is held to
 * {@code maxBodyBytes} when one is configured and to the fetcher's configured cap otherwise.
 * <p>
 * A {@code 2xx} body is emitted. Every {@code 4xx} the origin answers rejects with {@code null},
 * whatever the size of its body, so a map body drops a page that does not exist - and drops a
 * page the origin refused with a {@code 408} or a {@code 429} the same way. Every other failure
 * throws - a {@code 5xx}, a transport failure, a {@code 2xx} body past the cap, a request the
 * local rate limit refuses, a blank input, or an input that does not form a URI - so a collection
 * is never silently short a page because the server or the network failed.
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

        URI uri = URI.create(this.urlTemplate == null ? input : this.urlTemplate.replace(INPUT_MARKER, input));

        try {
            if (this.maxBodyBytes == null)
                return ctx.fetcher().get(uri).getBody();

            return ctx.fetcher().get(uri, this.maxBodyBytes).getBody();
        } catch (UrlFetchException.ClientError ignored) {
            return null;
        }
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
