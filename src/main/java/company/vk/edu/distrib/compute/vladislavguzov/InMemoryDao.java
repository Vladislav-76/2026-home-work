package company.vk.edu.distrib.compute.vladislavguzov;

import company.vk.edu.distrib.compute.Dao;

import java.io.IOException;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;

public class InMemoryDao implements Dao<byte[]> {

    private final Map<String, byte[]> storage = new ConcurrentHashMap<>();

    @Override
    public byte[] get(String key) throws NoSuchElementException, IllegalArgumentException, IOException {
        keyCheck(key);
        final byte[] value = storage.get(key);
        valueCheck(key, value);
        return value;
    }

    @Override
    public void upsert(String key, byte[] value) throws IllegalArgumentException, IOException {
        keyCheck(key);
        valueCheck(key, value);
        storage.put(key, value);
    }

    @Override
    public void delete(String key) throws IllegalArgumentException, IOException {
        keyCheck(key);
        storage.remove(key);
    }

    @Override
    public void close() {
        // nothing to close
    }

    private static void keyCheck(String key) throws IllegalArgumentException {
        if (key == null || key.isBlank()) {
            throw new IllegalArgumentException("key is null or blank");
        }
    }

    private static void valueCheck(String key, byte[] value) throws NoSuchElementException {
        if (value == null) {
            throw new NoSuchElementException("value is null for key: " + key);
        }
    }
}
