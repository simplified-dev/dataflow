package dev.sbs.dataflow.source;

import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

class UrlFetcherTest {

    private HttpServer server;
    private URI baseUri;
    private final HttpClient client = HttpClient.newBuilder()
        .connectTimeout(Duration.ofSeconds(2))
        .build();

    @BeforeEach
    void startServer() throws IOException {
        this.server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        this.server.createContext("/hello", exchange -> {
            byte[] body = "hello world".getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "text/plain; charset=UTF-8");
            exchange.sendResponseHeaders(200, body.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(body);
            }
        });
        this.server.createContext("/big", exchange -> {
            byte[] body = new byte[64 * 1024];
            java.util.Arrays.fill(body, (byte) 'a');
            exchange.getResponseHeaders().add("Content-Type", "text/plain");
            exchange.sendResponseHeaders(200, body.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(body);
            }
        });
        this.server.createContext("/bad", exchange -> {
            byte[] body = "nope".getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(503, body.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(body);
            }
        });
        this.server.start();
        this.baseUri = URI.create("http://127.0.0.1:" + this.server.getAddress().getPort());
    }

    @AfterEach
    void stopServer() {
        if (this.server != null) this.server.stop(0);
    }

    @Test
    @DisplayName("Fetches a small body and decodes by Content-Type charset")
    void fetchesSmallBody() throws Exception {
        UrlFetcher.Result result = UrlFetcher.of(this.client).fetch(this.baseUri.resolve("/hello"));
        assertThat(result.statusCode(), is(equalTo(200)));
        assertThat(result.body(), is(equalTo("hello world")));
        assertThat(result.contentType(), containsString("text/plain"));
    }

    @Test
    @DisplayName("Refuses bodies that exceed the configured cap")
    void refusesOversizedBody() {
        UrlFetcher fetcher = UrlFetcher.of(this.client, 1024, Duration.ofSeconds(2));
        try {
            fetcher.fetch(this.baseUri.resolve("/big"));
        } catch (IOException expected) {
            assertThat(expected.getMessage(), containsString("exceeded cap"));
            return;
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new AssertionError("Interrupted unexpectedly", ie);
        }
        throw new AssertionError("Expected IOException for oversized body");
    }

    @Test
    @DisplayName("Throws on non-2xx responses")
    void throwsOnFailureStatus() {
        try {
            UrlFetcher.of(this.client).fetch(this.baseUri.resolve("/bad"));
        } catch (IOException expected) {
            assertThat(expected.getMessage(), containsString("HTTP 503"));
            return;
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new AssertionError("Interrupted unexpectedly", ie);
        }
        throw new AssertionError("Expected IOException for HTTP 503");
    }

}
