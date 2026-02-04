FROM maven:3.9-eclipse-temurin-21 AS builder

WORKDIR /build
COPY pom.xml .
COPY common common
COPY runner runner
COPY ytdb ytdb

RUN mvn -pl runner -am package -Dmaven.test.skip=true -q

FROM eclipse-temurin:21-jre

WORKDIR /app
COPY --from=builder /build/runner/target/runner-1.0-SNAPSHOT.jar /app/runner.jar
COPY ytdb/ldbc-validation/driver.properties /app/driver.properties

ENTRYPOINT ["java", "--add-exports", "java.base/sun.nio.ch=ALL-UNNAMED", "-cp", "/app/runner.jar", "org.ldbcouncil.snb.driver.Client", "-P", "/app/driver.properties"]
