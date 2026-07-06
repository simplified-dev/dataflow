plugins {
    id("java-library")
}

group = "dev.sbs"
version = "0.1.0"

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(21))
    }
}

repositories {
    mavenCentral()
    maven(url = "https://jitpack.io")
}

dependencies {
    // JetBrains Annotations
    api(libs.annotations)

    // Gson (pipeline definition serde)
    api(libs.gson)

    // Jackson XML (XML -> JsonElement source bridge)
    api(libs.jackson.dataformat.xml)

    // Jsoup (HTML parsing + CSS selectors)
    api(libs.jsoup)

    // SLF4J (logging)
    api(libs.slf4j.api)

    // Simplified Libraries (extracted to github.com/simplified-dev)
    api("com.github.simplified-dev:client") { version { strictly("92c40b0") } }
    api("com.github.simplified-dev:collections") { version { strictly("2f2aa58") } }
    api("com.github.simplified-dev:gson-extras") { version { strictly("f42ee07") } }
    api("com.github.simplified-dev:reflection") { version { strictly("b2cf834") } }

    // Lombok Annotations
    compileOnly(libs.lombok)
    annotationProcessor(libs.lombok)
    testCompileOnly(libs.lombok)
    testAnnotationProcessor(libs.lombok)

    // Tests
    testImplementation(libs.hamcrest)
    testImplementation(libs.junit.jupiter.api)
    testRuntimeOnly(libs.junit.jupiter.engine)
    testImplementation(libs.junit.platform.launcher)
}

tasks.withType<JavaCompile>().configureEach {
    options.compilerArgs.add("-parameters")
}

tasks.test {
    useJUnitPlatform()
}
