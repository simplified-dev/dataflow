package dev.sbs.dataflow.stage.terminal.collect;

import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import dev.sbs.dataflow.PipelineContext;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

class JsonObjectFromEntriesCollectTest {

    @Test
    @DisplayName("Merges a list of {key,value} entries into one JsonObject keyed by key")
    void mergesEntries() {
        JsonObject a = new JsonObject();
        a.add("key", new JsonPrimitive("damage"));
        a.add("value", new JsonPrimitive(500));
        JsonObject b = new JsonObject();
        b.add("key", new JsonPrimitive("strength"));
        b.add("value", new JsonPrimitive(220));

        JsonObject merged = JsonObjectFromEntriesCollect.of()
            .execute(PipelineContext.empty(), List.of(a, b));

        assertThat(merged, is(notNullValue()));
        assertThat(merged.get("damage").getAsInt(), is(equalTo(500)));
        assertThat(merged.get("strength").getAsInt(), is(equalTo(220)));
    }

    @Test
    @DisplayName("Duplicate keys keep the last value seen")
    void duplicateKeyLastWins() {
        JsonObject first = new JsonObject();
        first.add("key", new JsonPrimitive("x"));
        first.add("value", new JsonPrimitive(1));
        JsonObject second = new JsonObject();
        second.add("key", new JsonPrimitive("x"));
        second.add("value", new JsonPrimitive(2));

        JsonObject merged = JsonObjectFromEntriesCollect.of()
            .execute(PipelineContext.empty(), List.of(first, second));

        assertThat(merged.get("x").getAsInt(), is(equalTo(2)));
    }

    @Test
    @DisplayName("Empty list produces an empty JsonObject")
    void emptyListProducesEmptyObject() {
        JsonObject merged = JsonObjectFromEntriesCollect.of()
            .execute(PipelineContext.empty(), List.of());

        assertThat(merged, is(notNullValue()));
        assertThat(merged.size(), is(equalTo(0)));
    }

    @Test
    @DisplayName("Null input returns null")
    void nullInputReturnsNull() {
        JsonObject merged = JsonObjectFromEntriesCollect.of()
            .execute(PipelineContext.empty(), null);
        assertThat(merged, is(nullValue()));
    }

    @Test
    @DisplayName("Entries missing key or value are skipped")
    void malformedEntriesSkipped() {
        JsonObject missingKey = new JsonObject();
        missingKey.add("value", new JsonPrimitive(1));
        JsonObject missingValue = new JsonObject();
        missingValue.add("key", new JsonPrimitive("y"));
        JsonObject nonStringKey = new JsonObject();
        nonStringKey.add("key", new JsonPrimitive(42));
        nonStringKey.add("value", new JsonPrimitive("x"));

        JsonObject merged = JsonObjectFromEntriesCollect.of()
            .execute(PipelineContext.empty(), List.of(missingKey, missingValue, nonStringKey));

        assertThat(merged.size(), is(equalTo(0)));
    }

}
