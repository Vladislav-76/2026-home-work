package company.vk.edu.distrib.compute.vladislav_guzov;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.AuditEvent;
import company.vk.edu.distrib.compute.AuditableKVService;
import company.vk.edu.distrib.compute.Dao;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.util.NoSuchElementException;
import java.util.Objects;

public class InMemoryKVService implements AuditableKVService {
    private static final Logger log = LoggerFactory.getLogger(InMemoryKVService.class);
    private static final String ID_PARAM_PREFIX = "id=";

    private final HttpServer server;
    private final Dao<byte[]> dao;
    private KafkaAuditSender sender;

    public InMemoryKVService(int port, Dao<byte[]> dao) throws IOException {
        this.server = HttpServer.create(new InetSocketAddress(port), 0);
        this.dao = dao;
        initServer();
    }

    @Override
    public void setBootstrapServers(String bootstrapServers) {
        if (sender != null) {
            sender.close();
        }
        sender = new KafkaAuditSender(bootstrapServers);
    }

    @Override
    public void setAsync(boolean enabled) {
        if (sender != null) {
            sender.setAsync(enabled);
        }
    }

    private void sendAudit(String method, String id) {
        if (sender != null) {
            sender.send(new AuditEvent(method, id, System.currentTimeMillis()));
        }
    }

    private void initServer() {
        server.createContext("/v0/status", http -> {
            if (Objects.equals("GET", http.getRequestMethod())) {
                http.sendResponseHeaders(200, 0);
            } else {
                http.sendResponseHeaders(405, 0);
            }
            http.close();
        });

        server.createContext("/v0/entity", new ErrorHttpHandler(http -> {
            String requestMethod = http.getRequestMethod();
            log.info("Method {}", requestMethod);
            final String id = parseId(http.getRequestURI().getQuery());
            sendAudit(requestMethod, id);
            final byte[] value;
            switch (requestMethod) {
                case "GET" -> {
                    value = dao.get(id);
                    http.sendResponseHeaders(200, value.length);
                    http.getResponseBody().write(value);
                }
                case "PUT" -> {
                    try (InputStream requestBody = http.getRequestBody()) {
                        dao.upsert(id, requestBody.readAllBytes());
                        http.sendResponseHeaders(201, 0);
                    }
                }
                case "DELETE" -> {
                    dao.delete(id);
                    http.sendResponseHeaders(202, 0);
                }
                default -> http.sendResponseHeaders(405, 0);
            }
            http.close();
        }));
    }

    private static String parseId(String query) {
        if (query != null && query.startsWith(ID_PARAM_PREFIX)) {
            return query.substring(ID_PARAM_PREFIX.length());
        }
        throw new IllegalArgumentException("Bad query");
    }

    @Override
    public void start() {
        log.info("Starting");
        server.start();
    }

    @Override
    public void stop() {
        log.info("Stopped");
        if (sender != null) {
            sender.close();
        }
        server.stop(1);
    }

    private static final class ErrorHttpHandler implements HttpHandler {
        private final HttpHandler delegate;

        private ErrorHttpHandler(HttpHandler delegate) {
            this.delegate = delegate;
        }

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            try (exchange) {
                try {
                    delegate.handle(exchange);
                } catch (IllegalArgumentException e) {
                    exchange.sendResponseHeaders(400, -1);
                } catch (NoSuchElementException e) {
                    exchange.sendResponseHeaders(404, -1);
                } catch (IOException | OutOfMemoryError e) {
                    exchange.sendResponseHeaders(500, -1);
                }
            }
        }
    }
}
