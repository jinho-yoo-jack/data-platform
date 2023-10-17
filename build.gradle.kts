plugins {
    java
    id("org.springframework.boot") version "2.7.15-SNAPSHOT"
    id("io.spring.dependency-management") version "1.0.15.RELEASE"
}

group = "kcp-data-platform"
version = "0.0.1-SNAPSHOT"

java {
    sourceCompatibility = JavaVersion.VERSION_1_8
}

configurations {
    compileOnly {
        extendsFrom(configurations.annotationProcessor.get())
    }
}

repositories {
    mavenCentral()
    maven { url = uri("https://repo.spring.io/milestone") }
    maven { url = uri("https://repo.spring.io/snapshot") }
}

dependencies {
    implementation("org.springframework.boot:spring-boot-starter-data-jpa")
    implementation("org.springframework.boot:spring-boot-starter-web")
    implementation("org.apache.kafka:kafka-streams")
    implementation("org.springframework.kafka:spring-kafka")
    implementation("com.squareup.retrofit2:retrofit:2.9.0")
    implementation("com.squareup.retrofit2:converter-gson:2.9.0")
    implementation("org.apache.parquet:parquet-hadoop:1.13.1")
    implementation("org.apache.parquet:parquet-avro:1.13.1")
    implementation("org.apache.spark:spark-core_2.13:3.4.1")
    implementation("com.oracle.database.jdbc:ojdbc8:21.1.0.0")
    implementation("com.googlecode.json-simple:json-simple:1.1.1")
    implementation("org.apache.spark:spark-sql_2.13:3.4.1")

    // https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-client
    implementation("org.apache.hadoop:hadoop-client:3.3.1")

    testImplementation("org.projectlombok:lombok:1.18.26")
    compileOnly("org.projectlombok:lombok")
    developmentOnly("org.springframework.boot:spring-boot-devtools")
    runtimeOnly("io.micrometer:micrometer-registry-prometheus")
    runtimeOnly("org.postgresql:postgresql")
    annotationProcessor("org.projectlombok:lombok")
    testAnnotationProcessor("org.projectlombok:lombok")
    testImplementation("org.springframework.boot:spring-boot-starter-test")
    testImplementation("org.springframework.kafka:spring-kafka-test")
}

tasks.withType<Test> {
    useJUnitPlatform()
}
