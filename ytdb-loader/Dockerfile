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
ENV YTDB_SERVER_HOST=localhost
ENV YTDB_SERVER_PORT=8182
ENV YTDB_SERVER_USER=root
ENV YTDB_SERVER_PASSWORD=root
ENV YTDB_DATABASE_NAME=ldbc_snb
ENV YTDB_DATABASE_USER=admin
ENV YTDB_DATABASE_PASSWORD=admin
ENV YTDB_DATASET_PATH=/data
# YTDB_BACKUP_PATH - optional, server-side path for backup/restore

VOLUME /data

ENTRYPOINT ["java", "-jar", "loader.jar"]
