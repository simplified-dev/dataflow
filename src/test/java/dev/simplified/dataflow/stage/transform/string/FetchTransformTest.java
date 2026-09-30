package dev.simplified.dataflow.stage.transform.string;

import com.google.gson.Gson;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import dev.simplified.client.exception.ErrorContext;
import dev.simplified.client.exception.UrlFetchException;
import dev.simplified.client.fetch.UrlFetcher;
import dev.simplified.client.fetch.UrlFetcherConfig;
import dev.simplified.client.ratelimit.RateLimit;
import dev.simplified.client.request.HttpMethod;
import dev.simplified.client.response.HttpStatus;
import dev.simplified.client.response.NetworkDetails;
import dev.simplified.dataflow.DataPipeline;
import dev.simplified.dataflow.DataType;
import dev.simplified.dataflow.DataTypes;
import dev.simplified.dataflow.FetchGuard;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Covers {@link FetchTransform} against a loopback server whose {@code /wiki/<name>} answers
 * {@code 200} with {@code page:<name>}, except {@code Missing} ({@code 404}), {@code Gone}
 * ({@code 410}), {@code Slow} ({@code 408}), {@code Busy} ({@code 429}), {@code Odd}
 * ({@code 460}, a code the client has no constant for), {@code Token} ({@code 498}, one in the
 * Nginx range the client has no constant for), {@code Closed} ({@code 499}, an Nginx code),
 * {@code Broken} ({@code 500}), {@code Moved} ({@code 302} to {@code Alpha}), {@code Choices}
 * ({@code 300}, which no fetcher follows) and {@code Unlisted} ({@code 299}, a {@code 2xx} the
 * client has no constant for, with {@code page:Unlisted}), whose {@code /cached/<name>} answers {@code 200} with {@code cached}, fresh for a minute, and whose
 * {@code /echo/<name>} answers {@code 200} with the raw path its request line carried.
 */
class FetchTransformTest {

    private HttpServer server;

    private String baseUrl;

    /**
     * The requests {@code /cached/} has answered.
     */
    private final AtomicInteger cachedHits = new AtomicInteger();

    @BeforeEach
    void startServer() throws IOException {
        this.server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        this.server.createContext("/wiki/", exchange -> {
            String name = exchange.getRequestURI().getPath().substring("/wiki/".length());

            switch (name) {
                case "Missing" -> respond(exchange, 404, "no such page");
                case "Gone" -> respond(exchange, 410, "deleted");
                case "Slow" -> respond(exchange, 408, "timed out");
                case "Busy" -> respond(exchange, 429, "slow down");
                case "Odd" -> respond(exchange, 460, "refused");
                case "Token" -> respond(exchange, 498, "invalid token");
                case "Closed" -> respond(exchange, 499, "closed");
                case "Broken" -> respond(exchange, 500, "down");
                case "Choices" -> respond(exchange, 300, "pick one");
                case "Unlisted" -> respond(exchange, 299, "page:Unlisted");
                case "Moved" -> {
                    exchange.getResponseHeaders().add("Location", "/wiki/Alpha");
                    exchange.sendResponseHeaders(302, -1);
                    exchange.close();
                }
                default -> respond(exchange, 200, "page:" + name);
            }
        });
        this.server.createContext("/cached/", exchange -> {
            this.cachedHits.incrementAndGet();
            exchange.getResponseHeaders().add("Cache-Control", "max-age=60");
            respond(exchange, 200, "cached");
        });
        this.server.createContext("/echo/", exchange -> respond(exchange, 200, exchange.getRequestURI().getRawPath()));
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

    private static @NotNull PipelineContext guarded(@NotNull FetchGuard guard) {
        UrlFetcher fetcher = UrlFetcher.create(UrlFetcherConfig.builder(new Gson()).build());
        return PipelineContext.builder().withFetcher(fetcher).withFetchGuard(guard).build();
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
    @DisplayName("A 4xx the client has no constant for rejects with null")
    void unknownClientErrorRejects() {
        assertThat(wiki().execute(context(), "Odd"), is(nullValue()));
    }

    @Test
    @DisplayName("An origin 408 throws its ClientError rather than dropping the element")
    void originRequestTimeoutThrows() {
        UrlFetchException.ClientError thrown = assertThrows(UrlFetchException.ClientError.class, () -> wiki().execute(context(), "Slow"));
        assertThat(thrown.getStatusCode(), is(408));
    }

    @Test
    @DisplayName("An origin 429 throws its ClientError rather than dropping the element")
    void originTooManyRequestsThrows() {
        UrlFetchException.ClientError thrown = assertThrows(UrlFetchException.ClientError.class, () -> wiki().execute(context(), "Busy"));
        assertThat(thrown.getStatusCode(), is(429));
    }

    @Test
    @DisplayName("As a map body an origin 429 fails the run rather than shortening the list")
    void mapBodyFailsOnTooManyRequests() {
        DataPipeline<List<String>> pipeline = pages("Alpha,Busy,Beta");
        assertThrows(UrlFetchException.ClientError.class, () -> pipeline.execute(context()));
    }

    @Test
    @DisplayName("As a map body an origin 408 fails the run rather than shortening the list")
    void mapBodyFailsOnRequestTimeout() {
        DataPipeline<List<String>> pipeline = pages("Alpha,Slow,Beta");
        assertThrows(UrlFetchException.ClientError.class, () -> pipeline.execute(context()));
    }

    @Test
    @DisplayName("The fetch guard sees the fetched URL and body")
    void guardSeesUrlAndBody() {
        List<String> seen = new ArrayList<>();
        PipelineContext ctx = guarded((uri, body) -> seen.add(uri + " -> " + body));

        wiki().execute(ctx, "Alpha");

        assertThat(seen, contains(this.baseUrl + "/wiki/Alpha -> page:Alpha"));
    }

    @Test
    @DisplayName("The fetch guard sees each element a map body fetches, in list order")
    void guardSeesEachElement() {
        List<String> seen = new ArrayList<>();
        pages("Gamma,Alpha").execute(guarded((uri, body) -> seen.add(body)));
        assertThat(seen, contains("page:Gamma", "page:Alpha"));
    }

    @Test
    @DisplayName("The fetch guard does not see a 4xx that rejects")
    void guardSkipsRejectedPage() {
        List<String> seen = new ArrayList<>();
        wiki().execute(guarded((uri, body) -> seen.add(body)), "Missing");
        assertThat(seen, is(empty()));
    }

    @Test
    @DisplayName("The fetch guard does not see a 429 that throws")
    void guardSkipsThrownClientError() {
        List<String> seen = new ArrayList<>();
        PipelineContext ctx = guarded((uri, body) -> seen.add(body));

        assertThrows(UrlFetchException.ClientError.class, () -> wiki().execute(ctx, "Busy"));

        assertThat(seen, is(empty()));
    }

    @Test
    @DisplayName("The fetch guard sees a body the response cache replays")
    void guardSeesCacheReplay() {
        List<String> seen = new ArrayList<>();
        PipelineContext ctx = guarded((uri, body) -> seen.add(body));
        FetchTransform stage = FetchTransform.of(DataTypes.RAW_HTML, this.baseUrl + "/cached/{}");

        stage.execute(ctx, "Alpha");
        stage.execute(ctx, "Alpha");

        assertThat(List.of(seen.size(), this.cachedHits.get()), contains(2, 1));
    }

    @Test
    @DisplayName("The fetch guard sees the URL the stage requested, not the one a redirect led to")
    void guardSeesRequestedUrlUnderRedirect() {
        List<String> seen = new ArrayList<>();
        wiki().execute(guarded((uri, body) -> seen.add(uri + " -> " + body)), "Moved");
        assertThat(seen, contains(this.baseUrl + "/wiki/Moved -> page:Alpha"));
    }

    @Test
    @DisplayName("A guard that throws fails the fetch with its exception")
    void guardThrowFails() {
        IllegalStateException refusal = new IllegalStateException("refused");
        PipelineContext ctx = guarded((uri, body) -> { throw refusal; });

        IllegalStateException thrown = assertThrows(IllegalStateException.class, () -> wiki().execute(ctx, "Alpha"));

        assertThat(thrown, is(sameInstance(refusal)));
    }

    @Test
    @DisplayName("A guard that throws a ClientError fails the fetch rather than dropping the element")
    void guardClientErrorFails() {
        UrlFetchException.ClientError refusal = new UrlFetchException.ClientError(
            new ErrorContext(HttpStatus.NOT_FOUND, HttpMethod.GET, this.baseUrl, Map.of(), Map.of(), new byte[0]),
            NetworkDetails.EMPTY
        );
        PipelineContext ctx = guarded((uri, body) -> { throw refusal; });

        UrlFetchException.ClientError thrown = assertThrows(UrlFetchException.ClientError.class, () -> wiki().execute(ctx, "Alpha"));

        assertThat(thrown, is(sameInstance(refusal)));
    }

    @Test
    @DisplayName("As a map body a guard that throws fails the run rather than shortening the list")
    void mapBodyFailsOnGuardThrow() {
        PipelineContext ctx = guarded((uri, body) -> {
            if (body.equals("page:Beta"))
                throw new IllegalStateException("refused");
        });

        assertThrows(IllegalStateException.class, () -> pages("Alpha,Beta,Gamma").execute(ctx));
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
    @DisplayName("An Nginx 4xx throws a UrlFetchException that is not a ClientError")
    void nginxClientCodeThrows() {
        UrlFetchException thrown = assertThrows(UrlFetchException.class, () -> wiki().execute(context(), "Closed"));
        assertThat(thrown, is(not(instanceOf(UrlFetchException.ClientError.class))));
    }

    @Test
    @DisplayName("A code in the Nginx range the client has no constant for throws as the Nginx codes do")
    void unknownNginxCodeThrows() {
        UrlFetchException thrown = assertThrows(UrlFetchException.class, () -> wiki().execute(context(), "Token"));
        assertThat(thrown, is(not(instanceOf(UrlFetchException.ClientError.class))));
    }

    @Test
    @DisplayName("A 3xx the fetcher does not follow throws, carrying its code, rather than emitting its body")
    void unfollowedRedirectionThrows() {
        UrlFetchException thrown = assertThrows(UrlFetchException.class, () -> wiki().execute(context(), "Choices"));
        assertThat(thrown.getStatusCode(), is(300));
    }

    @Test
    @DisplayName("As a map body a 3xx the fetcher does not follow fails the run")
    void mapBodyFailsOnUnfollowedRedirection() {
        DataPipeline<List<String>> pipeline = pages("Alpha,Choices,Beta");
        assertThrows(UrlFetchException.class, () -> pipeline.execute(context()));
    }

    @Test
    @DisplayName("A 2xx the client has no constant for is emitted")
    void unknownSuccessEmitted() {
        assertThat(wiki().execute(context(), "Unlisted"), is(equalTo("page:Unlisted")));
    }

    @Test
    @DisplayName("The fetch guard sees a body under a 2xx the client has no constant for")
    void guardSeesUnknownSuccess() {
        List<String> seen = new ArrayList<>();
        wiki().execute(guarded((uri, body) -> seen.add(body)), "Unlisted");
        assertThat(seen, contains("page:Unlisted"));
    }

    @Test
    @DisplayName("As a map body a 4xx the client has no constant for drops out")
    void mapBodyDropsUnknownClientError() {
        assertThat(pages("Alpha,Odd,Beta").execute(context()), contains("page:Alpha", "page:Beta"));
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
    @DisplayName("A character outside ASCII goes out as its UTF-8 bytes percent-encoded")
    void nonAsciiGoesOutPercentEncoded() {
        FetchTransform stage = FetchTransform.of(DataTypes.STRING, this.baseUrl + "/echo/{}");
        assertThat(stage.execute(context(), "Déjà_Vu"), is(equalTo("/echo/D%C3%A9j%C3%A0_Vu")));
    }

    @Test
    @DisplayName("A character outside Latin-1 fetches the page it names rather than one its name starts with")
    void nonLatin1FetchesNamedPage() {
        assertThat(wiki().execute(context(), "KnockOff™_Cola"), is(equalTo("page:KnockOff™_Cola")));
    }

    @Test
    @DisplayName("The fetch guard sees the URL in the ASCII form it was sent in")
    void guardSeesAsciiUrl() {
        List<String> seen = new ArrayList<>();
        wiki().execute(guarded((uri, body) -> seen.add(uri.toString())), "Déjà_Vu");
        assertThat(seen, contains(this.baseUrl + "/wiki/D%C3%A9j%C3%A0_Vu"));
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
