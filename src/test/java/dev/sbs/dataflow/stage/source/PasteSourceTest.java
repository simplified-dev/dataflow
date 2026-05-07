package dev.sbs.dataflow.stage.source;

import dev.sbs.dataflow.DataTypes;
import dev.sbs.dataflow.PipelineContext;
import dev.sbs.dataflow.stage.StageKind;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;

class PasteSourceTest {

    @Test
    @DisplayName("Paste source returns the body unchanged")
    void executeReturnsBody() {
        PasteSource source = PasteSource.text("hello");
        String result = source.execute(PipelineContext.empty(), null);
        assertThat(result, is(equalTo("hello")));
    }

    @Test
    @DisplayName("Paste source carries the chosen RAW_* output type")
    void outputTypeReflectsFlavor() {
        assertThat(PasteSource.html("x").outputType(), is(sameInstance(DataTypes.RAW_HTML)));
        assertThat(PasteSource.xml("x").outputType(), is(sameInstance(DataTypes.RAW_XML)));
        assertThat(PasteSource.json("x").outputType(), is(sameInstance(DataTypes.RAW_JSON)));
        assertThat(PasteSource.text("x").outputType(), is(sameInstance(DataTypes.RAW_TEXT)));
    }

    @Test
    @DisplayName("Paste source advertises StageKind.SOURCE_PASTE")
    void kindIsSourcePaste() {
        assertThat(PasteSource.text("x").kind(), is(equalTo(StageKind.SOURCE_PASTE)));
    }

}
