package dev.simplified.dataflow.stage.transform.string;

import com.google.gson.Gson;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import dev.simplified.client.exception.UrlFetchException;
import dev.simplified.client.fetch.UrlFetcher;
import dev.simplified.client.fetch.UrlFetcherConfig;
import dev.simplified.client.ratelimit.RateLimit;
import dev.simplified.dataflow.DataPipeline;
import dev.simplified.dataflow.DataType;
import dev.simplified.dataflow.DataTypes;
import dev.simplified.dataflow.PipelineContext;
import dev.simplified.dataflow.serde.PipelineGson;
import dev.simplified.dataflow.stage.source.LiteralSource;
import dev.simplified.dataflow.stage.transform.list.MapTransform;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.time.temporal.ChronoUnit;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Covers {@link FetchTransform} against a loopback server whose {@code /wiki/<name>} answers
 * {@code 200} with {@code page:<name>}, except {@code Missing} ({@code 404}), {@code Gone}
 * ({@code 410}), {@code Busy} ({@code 429}) and {@code Broken} ({@code 500}).
 */
class FetchTransformTest {

    private HttpServer server;

    private String baseUrl;

    @BeforeEach
    void startServer() throws IOException {
        this.server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        this.server.createContext("/wiki/", exchange -> {
            String name = exchange.getRequestURI().getPath().substring("/wiki/".length());

            switch (name) {
                case "Missing" -> respond(exchange, 404, "no such page");
                case "Gone" -> respond(exchange, 410, "deleted");
                case "Busy" -> respond(exchange, 429, "slow down");
                case "Broken" -> respond(exchange, 500, "down");
                default -> respond(exchange, 200, "page:" + name);
            }
        });
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

    private static @NotNull PipelineContext oneRequestPerHour() {
        UrlFetcher fetcher = UrlFetcher.create(
            UrlFetcherConfig.builder(new Gson()).withDefaultRateLimit(new RateLimit(1, 1, ChronoUnit.HOURS)).build()
        );
        return PipelineContext.builder().withFetcher(fetcher).build();
    }

    private @NotNull String template() {
        return this.baseUrl + "/wiki/{}";
    }

    private @NotNull FetchTransform wiki() {
        return FetchTransform.of(DataTypes.RAW_HTML, template());
    }

    private @NotNull DataPipeline<String> cappedPage() {
        return DataPipeline.builder()
            .source(LiteralSource.text(this.baseUrl + "/wiki/Alpha"))
            .stage(FetchTransform.of(DataTypes.RAW_HTML, null, 1024L))
            .build();
    }

    private @NotNull DataPipeline<List<String>> pages(@NotNull String names) {
        return DataPipeline.builder()
            .source(LiteralSource.text(names))
            .stage(SplitTransform.of(","))
            .stage(MapTransform.of(DataTypes.STRING, DataTypes.RAW_HTML, List.of(wiki())))
            .build();
    }

    private static @NotNull Throwable rootCause(@NotNull Throwable thrown) {
        Throwable root = thrown;
        while (root.getCause() != null && root.getCause() != root) root = root.getCause();
        return root;
    }

    @Test
    @DisplayName("A null input stays null")
    void nullInNullOut() {
        assertThat(wiki().execute(context(), null), is(nullValue()));
    }

    @Test
    @DisplayName("Without a template the input is the URL")
    void inputIsUrl() {
        assertThat(FetchTransform.of(DataTypes.RAW_HTML).execute(context(), this.baseUrl + "/wiki/Alpha"), is(equalTo("page:Alpha")));
    }

    @Test
    @DisplayName("The input replaces the template's {}")
    void templateSubstitutesInput() {
        assertThat(wiki().execute(context(), "Alpha"), is(equalTo("page:Alpha")));
    }

    @Test
    @DisplayName("The input replaces every {} in the template")
    void templateSubstitutesEveryMarker() {
        FetchTransform stage = FetchTransform.of(DataTypes.RAW_HTML, this.baseUrl + "/wiki/{}_{}");
        assertThat(stage.execute(context(), "A"), is(equalTo("page:A_A")));
    }

    @Test
    @DisplayName("A 404 rejects with null")
    void notFoundRejects() {
        assertThat(wiki().execute(context(), "Missing"), is(nullValue()));
    }

    @Test
    @DisplayName("A 410 rejects with null")
    void goneRejects() {
        assertThat(wiki().execute(context(), "Gone"), is(nullValue()));
    }

    @Test
    @DisplayName("An origin 429 rejects with null like every other 4xx")
    void originTooManyRequestsRejects() {
        assertThat(wiki().execute(context(), "Busy"), is(nullValue()));
    }

    @Test
    @DisplayName("A request the local rate limit refuses throws rather than dropping the element")
    void localRateLimitThrows() {
        PipelineContext limited = oneRequestPerHour();
        wiki().execute(limited, "Alpha");
        assertThrows(UrlFetchException.RateLimited.class, () -> wiki().execute(limited, "Beta"));
    }

    @Test
    @DisplayName("An empty input throws rather than fetching the template with nothing in place of {}")
    void emptyInputThrows() {
        assertThrows(IllegalArgumentException.class, () -> wiki().execute(context(), ""));
    }

    @Test
    @DisplayName("A blank input throws")
    void blankInputThrows() {
        assertThrows(IllegalArgumentException.class, () -> wiki().execute(context(), "   "));
    }

    @Test
    @DisplayName("As a map body an empty element fails the run rather than fetching the template alone")
    void mapBodyFailsOnEmptyElement() {
        DataPipeline<List<String>> pipeline = pages("Alpha,,Beta");
        assertThrows(IllegalArgumentException.class, () -> pipeline.execute(context()));
    }

    @Test
    @DisplayName("A 5xx throws a UrlFetchException that is not a ClientError")
    void serverErrorThrows() {
        UrlFetchException thrown = assertThrows(UrlFetchException.class, () -> wiki().execute(context(), "Broken"));
        assertThat(thrown, is(not(instanceOf(UrlFetchException.ClientError.class))));
    }

    @Test
    @DisplayName("A transport failure throws")
    void transportFailureThrows() throws IOException {
        int closedPort;

        try (ServerSocket socket = new ServerSocket(0)) {
            closedPort = socket.getLocalPort();
        }

        FetchTransform stage = FetchTransform.of(DataTypes.RAW_HTML, "http://127.0.0.1:" + closedPort + "/wiki/{}");
        assertThrows(UrlFetchException.Transport.class, () -> stage.execute(context(), "Alpha"));
    }

    @Test
    @DisplayName("An input that does not form a URI throws")
    void malformedUriThrows() {
        assertThrows(IllegalArgumentException.class, () -> wiki().execute(context(), "Alpha Beta"));
    }

    @Test
    @DisplayName("Without maxBodyBytes the fetch is held to the fetcher's configured cap")
    void absentCapUsesConfiguredCap() {
        assertThrows(UrlFetchException.BodyCapExceeded.class, () -> wiki().execute(context(4), "Alpha"));
    }

    @Test
    @DisplayName("maxBodyBytes below the body throws")
    void capBelowBodyThrows() {
        FetchTransform stage = FetchTransform.of(DataTypes.RAW_HTML, template(), 4L);
        assertThrows(UrlFetchException.BodyCapExceeded.class, () -> stage.execute(context(), "Alpha"));
    }

    @Test
    @DisplayName("A 404 whose page is larger than maxBodyBytes still rejects with null")
    void clientErrorPastTheCapRejects() {
        FetchTransform stage = FetchTransform.of(DataTypes.RAW_HTML, template(), 4L);
        assertThat(stage.execute(context(), "Missing"), is(nullValue()));
    }

    @Test
    @DisplayName("maxBodyBytes above the body lifts a smaller configured cap")
    void capOverridesConfiguredCap() {
        FetchTransform stage = FetchTransform.of(DataTypes.RAW_HTML, template(), 1024L);
        assertThat(stage.execute(context(4), "Alpha"), is(equalTo("page:Alpha")));
    }

    @Test
    @DisplayName("As a map body it fetches each element in list order")
    void mapBodyFetchesInOrder() {
        assertThat(pages("Gamma,Alpha,Beta").execute(context()), contains("page:Gamma", "page:Alpha", "page:Beta"));
    }

    @Test
    @DisplayName("As a map body a 4xx page drops out")
    void mapBodyDropsMissingPage() {
        assertThat(pages("Alpha,Missing,Beta").execute(context()), contains("page:Alpha", "page:Beta"));
    }

    @Test
    @DisplayName("As a map body a 5xx page fails the run")
    void mapBodyFailsOnServerError() {
        DataPipeline<List<String>> pipeline = pages("Alpha,Broken,Beta");
        assertThrows(UrlFetchException.class, () -> pipeline.execute(context()));
    }

    @Test
    @DisplayName("The stage reads STRING")
    void inputIsString() {
        assertThat(wiki().inputType(), is(sameInstance(DataTypes.STRING)));
    }

    @Test
    @DisplayName("The stage advertises its configured output type")
    void advertisesOutputType() {
        assertThat(FetchTransform.of(DataTypes.RAW_JSON).outputType(), is(sameInstance(DataTypes.RAW_JSON)));
    }

    @Test
    @DisplayName("of accepts STRING as the output type")
    void acceptsString() {
        assertThat(FetchTransform.of(DataTypes.STRING).outputType(), is(sameInstance(DataTypes.STRING)));
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Test
    @DisplayName("of refuses an output type that is not a string or raw type")
    void refusesOtherOutputType() {
        DataType<String> intAsRaw = (DataType) DataTypes.INT;
        assertThrows(IllegalArgumentException.class, () -> FetchTransform.of(intAsRaw));
    }

    @Test
    @DisplayName("of refuses a template with no {}")
    void refusesTemplateWithoutMarker() {
        assertThrows(IllegalArgumentException.class, () -> FetchTransform.of(DataTypes.RAW_HTML, "https://example.com/wiki/Alpha"));
    }

    @Test
    @DisplayName("of refuses a negative maxBodyBytes")
    void refusesNegativeCap() {
        assertThrows(IllegalArgumentException.class, () -> FetchTransform.of(DataTypes.RAW_HTML, template(), -1L));
    }

    @Test
    @DisplayName("config() omits an absent template and cap")
    void configOmitsAbsentSlots() {
        FetchTransform stage = FetchTransform.of(DataTypes.RAW_HTML);
        assertThat(List.of(stage.config().has("urlTemplate"), stage.config().has("maxBodyBytes")), contains(false, false));
    }

    @Test
    @DisplayName("The wire form carries every configured slot")
    void wireForm() {
        DataPipeline<String> pipeline = DataPipeline.builder()
            .source(LiteralSource.text("Alpha"))
            .stage(FetchTransform.of(DataTypes.RAW_HTML, "https://example.com/wiki/{}", 1024L))
            .build();
        assertThat(
            PipelineGson.toJson(pipeline),
            containsString("{\"kind\":\"TRANSFORM_FETCH\",\"outputType\":\"RAW_HTML\",\"urlTemplate\":\"https://example.com/wiki/{}\",\"maxBodyBytes\":1024}")
        );
    }

    @Test
    @DisplayName("A pipeline fanning out over pages round-trips to the same JSON")
    void wireRoundTripIsStable() {
        String first = PipelineGson.toJson(pages("Alpha,Missing,Beta"));
        assertThat(PipelineGson.toJson(PipelineGson.fromJson(first)), is(equalTo(first)));
    }

    @Test
    @DisplayName("A pipeline fanning out over pages round-trips to the same output")
    void wireRoundTripExecutes() {
        DataPipeline<?> rebuilt = PipelineGson.fromJson(PipelineGson.toJson(pages("Alpha,Missing,Beta")));
        assertThat(rebuilt.execute(context()), is(equalTo(pages("Alpha,Missing,Beta").execute(context()))));
    }

    @Test
    @DisplayName("A stage fetching its input under a cap round-trips to the same JSON")
    void cappedWireRoundTripIsStable() {
        String first = PipelineGson.toJson(cappedPage());
        assertThat(PipelineGson.toJson(PipelineGson.fromJson(first)), is(equalTo(first)));
    }

    @Test
    @DisplayName("A stage fetching its input under a cap round-trips to the same output")
    void cappedWireRoundTripExecutes() {
        DataPipeline<?> rebuilt = PipelineGson.fromJson(PipelineGson.toJson(cappedPage()));
        assertThat(rebuilt.execute(context(4)), is(equalTo(cappedPage().execute(context(4)))));
    }

    @Test
    @DisplayName("A template with no {} on the wire fails the load")
    void wireRejectsTemplateWithoutMarker() {
        String json = "[{\"kind\":\"SOURCE_LITERAL\",\"outputType\":\"STRING\",\"value\":\"a\"},"
            + "{\"kind\":\"TRANSFORM_FETCH\",\"outputType\":\"RAW_HTML\",\"urlTemplate\":\"https://example.com/x\"}]";
        Throwable thrown = assertThrows(RuntimeException.class, () -> PipelineGson.fromJson(json));
        assertThat(rootCause(thrown), is(instanceOf(IllegalArgumentException.class)));
    }

    @Test
    @DisplayName("The summary names the template and the cap")
    void summaryNamesTemplateAndCap() {
        assertThat(
            FetchTransform.of(DataTypes.RAW_HTML, "https://example.com/wiki/{}", 1024L).summary(),
            is(equalTo("Fetch RAW_HTML https://example.com/wiki/{} (cap 1024 bytes)"))
        );
    }

}
