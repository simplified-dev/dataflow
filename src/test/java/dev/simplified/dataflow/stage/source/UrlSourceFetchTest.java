package dev.simplified.dataflow.stage.source;

import com.google.gson.Gson;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import dev.simplified.client.exception.UrlFetchException;
import dev.simplified.client.fetch.UrlFetcher;
import dev.simplified.client.fetch.UrlFetcherConfig;
import dev.simplified.dataflow.DataPipeline;
import dev.simplified.dataflow.DataTypes;
import dev.simplified.dataflow.PipelineContext;
import dev.simplified.dataflow.serde.PipelineGson;
import dev.simplified.dataflow.stage.meta.StageReflection;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Covers the optional body cap of a {@link UrlSource} and how it reports an error status, against
 * a loopback server.
 */
class UrlSourceFetchTest {

    private static final @NotNull String BODY = "<html><body>ok</body></html>";

    private HttpServer server;

    private String baseUrl;

    @BeforeEach
    void startServer() throws IOException {
        this.server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        this.server.createContext("/page", exchange -> respond(exchange, 200, BODY));
        this.server.createContext("/missing", exchange -> respond(exchange, 404, "not here"));
        this.server.createContext("/broken", exchange -> respond(exchange, 500, "down"));
        this.server.start();
        this.baseUrl = "http://127.0.0.1:" + this.server.getAddress().getPort();
    }

    @AfterEach
    void stopServer() {
        if (this.server != null) this.server.stop(0);
    }

    private static void respond(@NotNull HttpExchange exchange, int status, @NotNull String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().add("Content-Type", "text/html; charset=UTF-8");
        exchange.sendResponseHeaders(status, bytes.length);

        try (OutputStream os = exchange.getResponseBody()) {
            os.write(bytes);
        }
    }

    private static @NotNull PipelineContext context(long configuredCap) {
        UrlFetcher fetcher = UrlFetcher.create(UrlFetcherConfig.builder(new Gson()).withMaxBodyBytes(configuredCap).build());
        return PipelineContext.builder().withFetcher(fetcher).build();
    }

    private static @NotNull PipelineContext context() {
        return context(UrlFetcherConfig.DEFAULT_MAX_BODY_BYTES);
    }

    private @NotNull String url(@NotNull String path) {
        return this.baseUrl + path;
    }

    @Test
    @DisplayName("Without maxBodyBytes the fetch is held to the fetcher's configured cap")
    void absentCapUsesConfiguredCap() {
        UrlSource source = UrlSource.rawHtml(url("/page"));
        assertThrows(UrlFetchException.BodyCapExceeded.class, () -> source.execute(context(8), null));
    }

    @Test
    @DisplayName("Without maxBodyBytes a body under the configured cap is emitted")
    void absentCapFetchesBody() {
        assertThat(UrlSource.rawHtml(url("/page")).execute(context(), null), is(equalTo(BODY)));
    }

    @Test
    @DisplayName("maxBodyBytes above the body lifts a smaller configured cap")
    void capOverridesConfiguredCap() {
        UrlSource source = UrlSource.of(DataTypes.RAW_HTML, url("/page"), 1024L);
        assertThat(source.execute(context(8), null), is(equalTo(BODY)));
    }

    @Test
    @DisplayName("maxBodyBytes below the body fails the fetch")
    void capBelowBodyThrows() {
        UrlSource source = UrlSource.of(DataTypes.RAW_HTML, url("/page"), 8L);
        assertThrows(UrlFetchException.BodyCapExceeded.class, () -> source.execute(context(), null));
    }

    @Test
    @DisplayName("maxBodyBytes equal to the body length admits it")
    void capAtBodyLengthAdmits() {
        long length = BODY.getBytes(StandardCharsets.UTF_8).length;
        assertThat(UrlSource.of(DataTypes.RAW_HTML, url("/page"), length).execute(context(), null), is(equalTo(BODY)));
    }

    @Test
    @DisplayName("of refuses a negative maxBodyBytes")
    void negativeCapRejected() {
        assertThrows(IllegalArgumentException.class, () -> UrlSource.of(DataTypes.RAW_HTML, "u", -1L));
    }

    @Test
    @DisplayName("A 4xx fails the run with a ClientError")
    void clientErrorThrows() {
        UrlSource source = UrlSource.rawHtml(url("/missing"));
        assertThrows(UrlFetchException.ClientError.class, () -> source.execute(context(), null));
    }

    @Test
    @DisplayName("A 5xx fails the run with a UrlFetchException that is not a ClientError")
    void serverErrorThrows() {
        UrlSource source = UrlSource.rawHtml(url("/broken"));
        UrlFetchException thrown = assertThrows(UrlFetchException.class, () -> source.execute(context(), null));
        assertThat(thrown, is(not(instanceOf(UrlFetchException.ClientError.class))));
    }

    @Test
    @DisplayName("The two-argument of leaves maxBodyBytes absent")
    void twoArgumentOfHasNoCap() {
        assertThat(UrlSource.of(DataTypes.RAW_HTML, "u").maxBodyBytes(), is(nullValue()));
    }

    @Test
    @DisplayName("config() omits an absent maxBodyBytes")
    void configOmitsAbsentCap() {
        assertThat(UrlSource.rawHtml("u").config().has("maxBodyBytes"), is(false));
    }

    @Test
    @DisplayName("config() carries a configured maxBodyBytes")
    void configCarriesCap() {
        assertThat(UrlSource.of(DataTypes.RAW_HTML, "u", 7L).config().getLong("maxBodyBytes"), is(equalTo(7L)));
    }

    @Test
    @DisplayName("fromConfig rebuilds the configured maxBodyBytes")
    void fromConfigRebuildsCap() {
        UrlSource original = UrlSource.of(DataTypes.RAW_HTML, "u", 7L);
        UrlSource rebuilt = (UrlSource) StageReflection.of(UrlSource.class).fromConfig(original.config());
        assertThat(rebuilt.maxBodyBytes(), is(equalTo(7L)));
    }

    @Test
    @DisplayName("The wire form carries maxBodyBytes as a number")
    void wireFormCarriesCap() {
        DataPipeline<String> pipeline = DataPipeline.builder().source(UrlSource.of(DataTypes.RAW_HTML, "u", 7L)).build();
        assertThat(PipelineGson.toJson(pipeline), containsString("\"maxBodyBytes\":7"));
    }

    @Test
    @DisplayName("The wire form of a source without a cap has no maxBodyBytes key")
    void wireFormOmitsAbsentCap() {
        DataPipeline<String> pipeline = DataPipeline.builder().source(UrlSource.rawHtml("u")).build();
        assertThat(PipelineGson.toJson(pipeline), is(equalTo("[{\"kind\":\"SOURCE_URL\",\"outputType\":\"RAW_HTML\",\"url\":\"u\"}]")));
    }

    @Test
    @DisplayName("A capped source round-trips to the same JSON")
    void wireRoundTripIsStable() {
        String first = PipelineGson.toJson(DataPipeline.builder().source(UrlSource.of(DataTypes.RAW_HTML, url("/page"), 1024L)).build());
        assertThat(PipelineGson.toJson(PipelineGson.fromJson(first)), is(equalTo(first)));
    }

    @Test
    @DisplayName("A capped source round-trips to the same output")
    void wireRoundTripExecutes() {
        String json = PipelineGson.toJson(DataPipeline.builder().source(UrlSource.of(DataTypes.RAW_HTML, url("/page"), 1024L)).build());
        assertThat(PipelineGson.fromJson(json).execute(context(8)), is(equalTo(BODY)));
    }

    @Test
    @DisplayName("A negative maxBodyBytes on the wire fails the load")
    void negativeCapRejectedAtLoad() {
        String json = "[{\"kind\":\"SOURCE_URL\",\"outputType\":\"RAW_HTML\",\"url\":\"u\",\"maxBodyBytes\":-1}]";
        Throwable thrown = assertThrows(RuntimeException.class, () -> PipelineGson.fromJson(json));
        assertThat(rootCause(thrown), is(instanceOf(IllegalArgumentException.class)));
    }

    @Test
    @DisplayName("The summary names a configured cap")
    void summaryNamesCap() {
        assertThat(UrlSource.of(DataTypes.RAW_HTML, "u", 7L).summary(), containsString("cap 7 bytes"));
    }

    private static @NotNull Throwable rootCause(@NotNull Throwable thrown) {
        Throwable root = thrown;
        while (root.getCause() != null && root.getCause() != root) root = root.getCause();
        return root;
    }

}
