FROM maven:3.9-eclipse-temurin-21 AS builder

WORKDIR /build
COPY pom.xml .

RUN mvn dependency:go-offline -q

COPY src src
RUN mvn package -DskipTests -q

FROM eclipse-temurin:21-jre-alpine

LABEL maintainer="YouTrackDB Team"
LABEL description="LDBC SNB data loader for YouTrackDB"

WORKDIR /app

COPY --from=builder /build/target/ytdb-loader-1.0-SNAPSHOT.jar loader.jar

ENV YTDB_MODE=remote

VOLUME /data

ENTRYPOINT ["java", "-jar", "loader.jar"]
