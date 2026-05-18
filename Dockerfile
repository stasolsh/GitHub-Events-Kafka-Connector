FROM gradle:8.10.2-jdk21 AS build
WORKDIR /app
COPY . .
RUN gradle clean jar --no-daemon

FROM confluentinc/cp-kafka-connect:7.7.1

USER root
RUN mkdir -p /usr/share/java/github-events-kafka-connector
COPY --from=build /app/build/libs/github-events-kafka-connector-1.0.0.jar /usr/share/java/github-events-kafka-connector/
USER appuser