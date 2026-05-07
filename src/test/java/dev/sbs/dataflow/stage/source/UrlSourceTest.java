package dev.sbs.dataflow.stage.source;

import com.sun.net.httpserver.HttpServer;
import dev.sbs.dataflow.DataTypes;
import dev.sbs.dataflow.PipelineContext;
import dev.sbs.dataflow.stage.StageKind;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;

class UrlSourceTest {

    private HttpServer server;
    private String baseUrl;

    @BeforeEach
    void startServer() throws IOException {
        this.server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        this.server.createContext("/page", exchange -> {
            byte[] body = "<html><body>ok</body></html>".getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "text/html; charset=UTF-8");
            exchange.sendResponseHeaders(200, body.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(body);
            }
        });
        this.server.start();
        this.baseUrl = "http://127.0.0.1:" + this.server.getAddress().getPort();
    }

    @AfterEach
    void stopServer() {
        if (this.server != null) this.server.stop(0);
    }

    @Test
    @DisplayName("UrlSource.html fetches and returns the body, advertising RAW_HTML")
    void fetchesHtmlBody() {
        UrlSource source = UrlSource.html(this.baseUrl + "/page");
        assertThat(source.outputType(), is(sameInstance(DataTypes.RAW_HTML)));
        assertThat(source.kind(), is(equalTo(StageKind.SOURCE_URL)));

        String body = source.execute(PipelineContext.empty(), null);
        assertThat(body, containsString("<body>ok</body>"));
    }

    @Test
    @DisplayName("Summary mentions the URL and the chosen RAW flavour")
    void summaryDescribesConfig() {
        UrlSource source = UrlSource.json("https://example.com/data");
        assertThat(source.summary(), containsString("RAW_JSON"));
        assertThat(source.summary(), containsString("https://example.com/data"));
    }

}
