plugins {
    // java-library, not application: this produces a reusable .jar with a proper
    // api/implementation split, not an executable with a main class.
    `java-library`
    // Lets the artifact be published — to the local Maven cache for use from another
    // project on this machine, and, when pushed to GitHub, consumed via JitPack.
    `maven-publish`
}

group = "dev.poliakov"
version = "0.1.0"

// Versions are pinned to exactly what the parent LatencyArbitrageBot resolves in
// production through the Spring Boot BOM, so this library compiles and runs against
// the same bytes the code was written and tested against — not a hopeful newer set.
val reactorCoreVersion = "3.8.5"
val reactorNettyHttpVersion = "1.3.5"
val jacksonVersion = "2.21.2"
val slf4jVersion = "2.0.17"
val web3jVersion = "4.12.0"
val junitVersion = "5.11.4"

java {
    // 21: the code uses records, sealed interfaces and pattern-matching switch.
    toolchain {
        languageVersion = JavaLanguageVersion.of(21)
    }
    // Both jars are part of a credible published library — sources so consumers can
    // step into the code, javadoc so the IDE shows the interface docs.
    withSourcesJar()
    withJavadocJar()
}

repositories {
    mavenCentral()
}

dependencies {
    // api = leaks through the public type surface, so a consumer gets it transitively
    // and compiles against it. Every port returns Mono/Flux, and the WS/REST clients
    // take a reactor-netty HttpClient in their public constructors, so both are api.
    api("io.projectreactor:reactor-core:$reactorCoreVersion")
    api("io.projectreactor.netty:reactor-netty-http:$reactorNettyHttpVersion")

    // implementation = internal only. Jackson is used to (de)serialize the private
    // REST/WS DTOs; web3j signs orders (keccak256 + secp256k1). Neither appears in a
    // public signature, so consumers neither see nor compile against them.
    // The BOM aligns the three Jackson modules, which are not all on the same number —
    // jackson-annotations trails at 2.21 while core/databind are at 2.21.2.
    implementation(platform("com.fasterxml.jackson:jackson-bom:$jacksonVersion"))
    implementation("com.fasterxml.jackson.core:jackson-databind")
    implementation("com.fasterxml.jackson.core:jackson-core")
    implementation("com.fasterxml.jackson.core:jackson-annotations")
    implementation("org.web3j:crypto:$web3jVersion")

    // slf4j-api only, never a binding: a library must not choose the consumer's logger.
    implementation("org.slf4j:slf4j-api:$slf4jVersion")

    testImplementation(platform("org.junit:junit-bom:$junitVersion"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
    testImplementation("io.projectreactor:reactor-test:$reactorCoreVersion")
}

tasks.withType<Test> {
    useJUnitPlatform()
}

tasks.withType<Javadoc> {
    // The value of this code is in its extensive `//` reasoning, which javadoc does not
    // read; doclint would otherwise fail the build over missing @param tags on internal
    // types. Turn it off rather than paper the code with ceremony it does not need.
    (options as StandardJavadocDocletOptions).addStringOption("Xdoclint:none", "-quiet")
}

publishing {
    publications {
        create<MavenPublication>("mavenJava") {
            from(components["java"])
        }
    }
}
