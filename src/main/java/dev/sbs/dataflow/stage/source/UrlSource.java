package dev.sbs.dataflow.stage.source;

import dev.sbs.dataflow.DataType;
import dev.sbs.dataflow.DataTypes;
import dev.sbs.dataflow.PipelineContext;
import dev.sbs.dataflow.source.UrlFetcher;
import dev.sbs.dataflow.stage.SourceStage;
import dev.sbs.dataflow.stage.StageId;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.UncheckedIOException;
import java.net.URI;

/**
 * {@link SourceStage} that fetches a URL via {@link UrlFetcher} and emits the response body
 * tagged as one of the {@code RAW_*} types.
 */
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
@Getter
@Accessors(fluent = true)
public final class UrlSource implements SourceStage<String> {

    private final @NotNull String url;
    private final @NotNull DataType<String> outputType;

    /**
     * Constructs a URL source whose body is treated as HTML.
     *
     * @param url the URL to fetch
     * @return a new source
     */
    public static @NotNull UrlSource html(@NotNull String url) {
        return new UrlSource(url, DataTypes.RAW_HTML);
    }

    /**
     * Constructs a URL source whose body is treated as XML.
     *
     * @param url the URL to fetch
     * @return a new source
     */
    public static @NotNull UrlSource xml(@NotNull String url) {
        return new UrlSource(url, DataTypes.RAW_XML);
    }

    /**
     * Constructs a URL source whose body is treated as JSON.
     *
     * @param url the URL to fetch
     * @return a new source
     */
    public static @NotNull UrlSource json(@NotNull String url) {
        return new UrlSource(url, DataTypes.RAW_JSON);
    }

    /**
     * Constructs a URL source whose body is treated as plain text.
     *
     * @param url the URL to fetch
     * @return a new source
     */
    public static @NotNull UrlSource text(@NotNull String url) {
        return new UrlSource(url, DataTypes.RAW_TEXT);
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull StageId kind() {
        return StageId.SOURCE_URL;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull String summary() {
        return "URL " + this.outputType.label() + " " + this.url;
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable String execute(@NotNull PipelineContext ctx, @Nullable Void input) {
        try {
            return UrlFetcher.of(ctx.http()).fetch(URI.create(this.url)).body();
        } catch (java.io.IOException e) {
            throw new UncheckedIOException(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Fetch interrupted", e);
        }
    }

}
