/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.connect.coordinator;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Executors;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simple HTTP server providing health check endpoints for coordinator pods.
 * Provides /health (liveness) and /ready (readiness) endpoints for Kubernetes.
 */
public class HealthCheckServer {

    private static final Logger LOG = LoggerFactory.getLogger(HealthCheckServer.class);

    private final int port;
    private final Supplier<Boolean> livenessCheck;
    private final Supplier<Boolean> readinessCheck;
    private HttpServer server;
    private volatile boolean running = false;

    public HealthCheckServer(int port, Supplier<Boolean> livenessCheck, Supplier<Boolean> readinessCheck) {
        this.port = port;
        this.livenessCheck = livenessCheck;
        this.readinessCheck = readinessCheck;
    }

    /**
     * Start the health check server.
     */
    public void start() throws IOException {
        if (running) {
            LOG.warn("Health check server is already running on port {}", port);
            return;
        }

        server = HttpServer.create(new InetSocketAddress(port), 0);

        // Health check endpoint (liveness probe)
        server.createContext("/health", new HealthHandler(livenessCheck, "health"));

        // Readiness check endpoint (readiness probe)
        server.createContext("/ready", new HealthHandler(readinessCheck, "readiness"));

        // Status endpoint with detailed information
        server.createContext("/status", new StatusHandler());

        server.setExecutor(Executors.newFixedThreadPool(2));
        server.start();

        running = true;
        LOG.info("Health check server started on port {}", port);
    }

    /**
     * Stop the health check server.
     */
    public void stop() {
        if (!running || server == null) {
            return;
        }

        LOG.info("Stopping health check server on port {}", port);
        server.stop(5); // Wait up to 5 seconds for existing requests
        running = false;
        LOG.info("Health check server stopped");
    }

    /**
     * Check if the server is running.
     */
    public boolean isRunning() {
        return running;
    }

    private static class HealthHandler implements HttpHandler {
        private final Supplier<Boolean> healthCheck;
        private final String checkType;

        public HealthHandler(Supplier<Boolean> healthCheck, String checkType) {
            this.healthCheck = healthCheck;
            this.checkType = checkType;
        }

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            try {
                boolean healthy = healthCheck != null ? healthCheck.get() : true;
                int statusCode = healthy ? 200 : 503;
                String response = String.format("{\"%s\": \"%s\"}",
                    checkType, healthy ? "UP" : "DOWN");

                exchange.getResponseHeaders().set("Content-Type", "application/json");
                exchange.sendResponseHeaders(statusCode, response.length());

                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(response.getBytes(StandardCharsets.UTF_8));
                }
            } catch (Exception e) {
                LOG.error("Error processing {} check", checkType, e);
                String errorResponse = String.format("{\"%s\": \"ERROR\", \"error\": \"%s\"}",
                    checkType, e.getMessage());
                exchange.sendResponseHeaders(500, errorResponse.length());
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(errorResponse.getBytes(StandardCharsets.UTF_8));
                }
            }
        }
    }

    private class StatusHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            try {
                boolean live = livenessCheck != null ? livenessCheck.get() : true;
                boolean ready = readinessCheck != null ? readinessCheck.get() : true;

                String response = String.format(
                    "{\"health\": \"%s\", \"readiness\": \"%s\", \"port\": %d, \"running\": %s}",
                    live ? "UP" : "DOWN",
                    ready ? "UP" : "DOWN",
                    port,
                    running
                );

                exchange.getResponseHeaders().set("Content-Type", "application/json");
                exchange.sendResponseHeaders(200, response.length());

                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(response.getBytes(StandardCharsets.UTF_8));
                }
            } catch (Exception e) {
                LOG.error("Error processing status check", e);
                String errorResponse = String.format("{\"error\": \"%s\"}", e.getMessage());
                exchange.sendResponseHeaders(500, errorResponse.length());
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(errorResponse.getBytes(StandardCharsets.UTF_8));
                }
            }
        }
    }
}