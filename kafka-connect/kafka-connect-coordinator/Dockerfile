FROM openjdk:17-jre-slim

# Install dependencies
RUN apt-get update && apt-get install -y \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Create app directory
WORKDIR /app

# Copy the JAR file
COPY build/libs/iceberg-kafka-connect-coordinator-*.jar app.jar

# Create non-root user
RUN groupadd -r iceberg && useradd -r -g iceberg iceberg
RUN chown -R iceberg:iceberg /app
USER iceberg

# Expose port (optional, for metrics)
EXPOSE 8080

# Set default environment variables
ENV KAFKA_BOOTSTRAP_SERVERS=localhost:9092
ENV CONNECT_GROUP_ID=iceberg-connect-group
ENV CONTROL_TOPIC=iceberg-control-topic
ENV CATALOG_NAME=iceberg_catalog

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=30s --retries=3 \
  CMD ps aux | grep '[I]cebergJobManagerApplication' | wc -l | grep -q 1 || exit 1

# Run the application
ENTRYPOINT ["java", "-jar", "app.jar"]