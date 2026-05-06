package dev.sbs.dataflow.source;

import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

/**
 * Thin {@link HttpClient} wrapper that fetches a URL into a single decoded {@link String}
 * with a configurable size cap and minimal content-type sniffing.
 * <p>
 * Used by {@link dev.sbs.dataflow.stage.source.UrlSource}; not a {@link dev.sbs.dataflow.stage.Stage}
 * itself so that other components can reuse the fetcher (e.g. live-preview caching) without
 * re-implementing the size cap.
 */
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class UrlFetcher {

    /** Default body size cap, 5 MiB. */
    public static final long DEFAULT_MAX_BYTES = 5L * 1024 * 1024;

    /** Default per-request timeout, 30 seconds. */
    public static final @NotNull Duration DEFAULT_TIMEOUT = Duration.ofSeconds(30);

    private final @NotNull HttpClient http;
    private final long maxBytes;
    private final @NotNull Duration timeout;

    /**
     * Constructs a fetcher with the given HTTP client and the {@link #DEFAULT_MAX_BYTES default cap}.
     *
     * @param http the HTTP client to use
     * @return a new fetcher
     */
    public static @NotNull UrlFetcher of(@NotNull HttpClient http) {
        return new UrlFetcher(http, DEFAULT_MAX_BYTES, DEFAULT_TIMEOUT);
    }

    /**
     * Constructs a fetcher with the given HTTP client, body size cap, and timeout.
     *
     * @param http the HTTP client to use
     * @param maxBytes the body cap in bytes; reads beyond this point throw
     * @param timeout the per-request timeout
     * @return a new fetcher
     */
    public static @NotNull UrlFetcher of(@NotNull HttpClient http, long maxBytes, @NotNull Duration timeout) {
        return new UrlFetcher(http, maxBytes, timeout);
    }

    /**
     * Fetches {@code uri} and returns the decoded body as a string.
     *
     * @param uri the absolute URI to fetch
     * @return the result, never {@code null}
     * @throws IOException if the response is not 2xx, the body exceeds the cap, or the network fails
     * @throws InterruptedException if the calling thread is interrupted
     */
    public @NotNull Result fetch(@NotNull URI uri) throws IOException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder(uri)
            .timeout(this.timeout)
            .header("Accept", "*/*")
            .header("User-Agent", "dataflow-fetcher/0.1")
            .GET()
            .build();

        HttpResponse<InputStream> response = this.http.send(request, HttpResponse.BodyHandlers.ofInputStream());
        int status = response.statusCode();
        if (status < 200 || status >= 300)
            throw new IOException("HTTP " + status + " from " + uri);

        String contentType = response.headers().firstValue("Content-Type").orElse("application/octet-stream");
        Charset charset = charsetFromContentType(contentType, StandardCharsets.UTF_8);

        try (InputStream in = response.body()) {
            byte[] bytes = readAllCapped(in, this.maxBytes);
            return new Result(new String(bytes, charset), contentType, status);
        }
    }

    private static byte @NotNull [] readAllCapped(@NotNull InputStream in, long maxBytes) throws IOException {
        byte[] buffer = new byte[8192];
        java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
        long total = 0;
        int read;
        while ((read = in.read(buffer)) != -1) {
            total += read;
            if (total > maxBytes)
                throw new IOException("Response body exceeded cap of " + maxBytes + " bytes");
            out.write(buffer, 0, read);
        }
        return out.toByteArray();
    }

    private static @NotNull Charset charsetFromContentType(@Nullable String contentType, @NotNull Charset fallback) {
        if (contentType == null) return fallback;
        int idx = contentType.toLowerCase().indexOf("charset=");
        if (idx < 0) return fallback;
        String value = contentType.substring(idx + "charset=".length()).trim();
        int semicolon = value.indexOf(';');
        if (semicolon >= 0) value = value.substring(0, semicolon).trim();
        if (value.startsWith("\"") && value.endsWith("\"") && value.length() >= 2)
            value = value.substring(1, value.length() - 1);
        try {
            return Charset.forName(value);
        } catch (Exception ignored) {
            return fallback;
        }
    }

    /**
     * Outcome of a successful fetch.
     *
     * @param body the decoded body
     * @param contentType the response {@code Content-Type} header, or {@code "application/octet-stream"} when absent
     * @param statusCode the HTTP status code
     */
    public record Result(@NotNull String body, @NotNull String contentType, int statusCode) {}

}
