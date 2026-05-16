package dev.sbs.dataflow.stage;

import dev.sbs.dataflow.stage.meta.StageSpec;
import dev.simplified.reflection.Reflection;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Classpath-scanning registry of every {@link Stage} implementation discovered under the
 * {@code dev.sbs.dataflow.stage} package.
 * <p>
 * The registry is built once on first touch: it asks {@link Reflection#getResources()} for
 * every subtype of {@link Stage}, filters those that carry a {@link StageSpec} annotation
 * (skipping the sealed parent interfaces {@code SourceStage} / {@code FilterStage} /
 * {@code TransformStage} / {@code CollectStage}), and indexes them by their stable wire-format
 * {@link StageSpec#id() id}.
 * <p>
 * The {@link StageSpec#id() id} string is the on-disk discriminator: writing a stage emits it as
 * the {@code "kind"} JSON field, and reading back uses {@link #byId(String)} to resolve the
 * implementation class. Two stages with the same id is a hard error - the static initialiser
 * throws {@link IllegalStateException}, which surfaces as a {@link ExceptionInInitializerError}
 * to callers.
 * <p>
 * Ordering: {@link #all()} reflects classpath-scan discovery order (undefined; do not rely on it).
 * {@link #allOrdered()} sorts by {@link StageSpec.Category#ordinal()} then case-insensitive
 * {@link StageSpec#displayName()}; UI palettes should use this view.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class StageRegistry {

    private static final @NotNull Map<String, Class<? extends Stage<?, ?>>> BY_ID;
    private static final @NotNull List<Class<? extends Stage<?, ?>>> ALL;
    private static final @NotNull List<Class<? extends Stage<?, ?>>> ORDERED;

    static {
        Map<String, Class<? extends Stage<?, ?>>> byId = new LinkedHashMap<>();
        List<Class<? extends Stage<?, ?>>> all = new ArrayList<>();

        for (Class<? extends Stage> rawCls : Reflection.getResources()
            .filterPackage("dev.sbs.dataflow.stage")
            .getSubtypesOf(Stage.class)) {
            StageSpec spec = rawCls.getAnnotation(StageSpec.class);
            if (spec == null) continue; // sealed parents (SourceStage / FilterStage / TransformStage / CollectStage)
            @SuppressWarnings("unchecked")
            Class<? extends Stage<?, ?>> cls = (Class<? extends Stage<?, ?>>) rawCls;
            Class<? extends Stage<?, ?>> previous = byId.putIfAbsent(spec.id(), cls);
            if (previous != null)
                throw new IllegalStateException(
                    "Duplicate @StageSpec id '" + spec.id() + "' on " + cls.getName() +
                        " - already bound to " + previous.getName()
                );
            all.add(cls);
        }

        BY_ID = Map.copyOf(byId);
        ALL = List.copyOf(all);
        ORDERED = all.stream()
            .sorted(Comparator
                .<Class<? extends Stage<?, ?>>, Integer>comparing(c -> c.getAnnotation(StageSpec.class).category().ordinal())
                .thenComparing(c -> c.getAnnotation(StageSpec.class).displayName().toLowerCase()))
            .toList();
    }

    /**
     * Returns the implementation class bound to {@code id}.
     *
     * @param id the wire-format discriminator, as declared by {@link StageSpec#id()}
     * @return the implementation class
     * @throws IllegalArgumentException when no stage is registered under {@code id}
     */
    public static @NotNull Class<? extends Stage<?, ?>> byId(@NotNull String id) {
        Class<? extends Stage<?, ?>> cls = BY_ID.get(id);
        if (cls == null)
            throw new IllegalArgumentException("No stage registered with id: " + id);
        return cls;
    }

    /**
     * Returns every registered stage class in classpath-scan discovery order.
     * <p>
     * Discovery order is implementation-defined; if you need stable ordering use
     * {@link #allOrdered()}.
     *
     * @return the registered stage classes
     */
    public static @NotNull List<Class<? extends Stage<?, ?>>> all() {
        return ALL;
    }

    /**
     * Returns every registered stage class sorted by {@link StageSpec.Category#ordinal()} then
     * case-insensitive {@link StageSpec#displayName()}.
     *
     * @return the registered stage classes, ordered for UI display
     */
    public static @NotNull List<Class<? extends Stage<?, ?>>> allOrdered() {
        return ORDERED;
    }

    /**
     * Returns every registered stage class whose {@link StageSpec#category()} matches
     * {@code category}, preserving the {@link #allOrdered()} sort order within the slice.
     *
     * @param category the category to filter by
     * @return the matching stage classes, possibly empty
     */
    public static @NotNull List<Class<? extends Stage<?, ?>>> ofCategory(@NotNull StageSpec.Category category) {
        return ORDERED.stream()
            .filter(c -> c.getAnnotation(StageSpec.class).category() == category)
            .toList();
    }

}
