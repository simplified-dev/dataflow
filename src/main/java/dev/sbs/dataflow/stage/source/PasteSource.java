package dev.sbs.dataflow.stage.source;

import dev.sbs.dataflow.DataType;
import dev.sbs.dataflow.DataTypes;
import dev.sbs.dataflow.PipelineContext;
import dev.sbs.dataflow.stage.SourceStage;
import dev.sbs.dataflow.stage.StageConfig;
import dev.sbs.dataflow.stage.StageKind;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * {@link SourceStage} that emits an inline string body, tagged as one of the {@code RAW_*}
 * types. Useful for unit tests and for the Discord builder UI's paste-input flow.
 */
@Getter
@Accessors(fluent = true)
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class PasteSource implements SourceStage<String> {

    private final @NotNull String body;

    private final @NotNull DataType<String> outputType;

    /**
     * Constructs a paste source whose body is treated as HTML.
     *
     * @param body the inline body
     * @return a new source
     */
    public static @NotNull PasteSource html(@NotNull String body) {
        return new PasteSource(body, DataTypes.RAW_HTML);
    }

    /**
     * Constructs a paste source whose body is treated as JSON.
     *
     * @param body the inline body
     * @return a new source
     */
    public static @NotNull PasteSource json(@NotNull String body) {
        return new PasteSource(body, DataTypes.RAW_JSON);
    }

    /**
     * Constructs a paste source whose body is treated as plain text.
     *
     * @param body the inline body
     * @return a new source
     */
    public static @NotNull PasteSource text(@NotNull String body) {
        return new PasteSource(body, DataTypes.RAW_TEXT);
    }

    /**
     * Constructs a paste source whose body is treated as XML.
     *
     * @param body the inline body
     * @return a new source
     */
    public static @NotNull PasteSource xml(@NotNull String body) {
        return new PasteSource(body, DataTypes.RAW_XML);
    }

    @SuppressWarnings("unchecked")
    public static @NotNull PasteSource fromConfig(@NotNull StageConfig cfg) {
        return new PasteSource(cfg.getString("body"), (DataType<String>) cfg.getDataType("outputType"));
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull StageConfig config() {
        return StageConfig.builder()
            .string("body", this.body)
            .dataType("outputType", this.outputType)
            .build();
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull String execute(@NotNull PipelineContext ctx, @Nullable Void input) {
        return this.body;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull StageKind kind() {
        return StageKind.SOURCE_PASTE;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull String summary() {
        return "Paste " + this.outputType.label() + " (" + this.body.length() + " chars)";
    }

}
