package company.vk.edu.distrib.compute.vladislavguzov;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.KVService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.util.NoSuchElementException;
import java.util.Objects;

public class InMemoryKVService implements KVService {
    private static final Logger log = LoggerFactory.getLogger(InMemoryKVService.class);
    private static final String ID_PARAM_PREFIX = "id=";

    private final HttpServer server;
    private final Dao<byte[]> dao;

    public InMemoryKVService(int port, Dao<byte[]> dao) throws IOException {
        this.server = HttpServer.create(new InetSocketAddress(port), 0);
        this.dao = dao;
        initServer();
    }

    private void initServer() {
        server.createContext("/v0/status", http -> {
            String requestMethod = http.getRequestMethod();
            if (Objects.equals("GET", requestMethod)) {
                http.sendResponseHeaders(200, 0);
            } else {
                http.sendResponseHeaders(405, 0);
            }
            http.close();
        });

        server.createContext("/v0/entity", new ErrorHttpHandler(http -> {
            String requestMethod = http.getRequestMethod();
            log.info("Method {}", requestMethod);
            String query = http.getRequestURI().getQuery();
            final String id = parseId(query);
            final byte[] value;
            switch (requestMethod) {
                case "GET":
                    value = dao.get(id);
                    http.sendResponseHeaders(200, value.length);
                    http.getResponseBody().write(value);
                    break;
                case "PUT":
                    InputStream requestBody = http.getRequestBody();
                    value = requestBody.readAllBytes();
                    requestBody.close();
                    dao.upsert(id, value);
                    http.sendResponseHeaders(201, 0);
                    break;
                case "DELETE":
                    dao.delete(id);
                    http.sendResponseHeaders(202, 0);
                    break;
                default:
                    http.sendResponseHeaders(405, 0);
                    break;
            }
            http.close();
        }));
    }

    private static String parseId(String query) {
        if (query.startsWith(ID_PARAM_PREFIX)) {
            return query.substring(ID_PARAM_PREFIX.length());
        } else {
            throw new IllegalArgumentException("Bad query");
        }
    }

    @Override
    public void start() {
        log.info("Starting");
        server.start();
    }

    @Override
    public void stop() {
        log.info("Stopped");
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
                    exchange.sendResponseHeaders(404,-1);
                } catch (IOException | OutOfMemoryError e) {
                    exchange.sendResponseHeaders(500,-1);
                }
            }
        }
    }
}
