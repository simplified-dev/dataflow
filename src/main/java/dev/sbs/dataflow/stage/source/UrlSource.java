package dev.sbs.dataflow.stage.source;

import dev.sbs.dataflow.DataType;
import dev.sbs.dataflow.DataTypes;
import dev.sbs.dataflow.PipelineContext;
import dev.sbs.dataflow.stage.Configurable;
import dev.sbs.dataflow.stage.SourceStage;
import dev.sbs.dataflow.stage.StageSpec;
import dev.simplified.client.fetch.UrlFetcher;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.net.URI;

/**
 * {@link SourceStage} that fetches a URL via {@link UrlFetcher} and emits the response body
 * tagged as one of the {@code RAW_*} types.
 */
@StageSpec(
    id = "SOURCE_URL",
    displayName = "URL Source",
    description = "() -> RAW_*",
    category = StageSpec.Category.SOURCE
)
@Getter
@Accessors(fluent = true)
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class UrlSource implements SourceStage<String> {

    private final @NotNull String url;

    private final @NotNull DataType<String> outputType;

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
     * @return a new source
     * @throws IllegalArgumentException when {@code outputType} is not one of the supported types
     */
    public static @NotNull UrlSource of(
        @Configurable(label = "Output type (RAW_HTML / RAW_XML / RAW_JSON / STRING)", placeholder = "RAW_HTML")
        @NotNull DataType<String> outputType,
        @Configurable(label = "URL", placeholder = "https://example.com/page")
        @NotNull String url
    ) {
        if (!SUPPORTED_OUTPUT_TYPES.contains(outputType))
            throw new IllegalArgumentException(
                "UrlSource supports " + SUPPORTED_OUTPUT_TYPES + " but got " + outputType
            );
        return new UrlSource(url, outputType);
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
        return ctx.fetcher().get(URI.create(this.url)).getBody();
    }
    /** {@inheritDoc} */
    @Override
    public @NotNull String summary() {
        return "URL " + this.outputType.label() + " " + this.url;
    }

}
