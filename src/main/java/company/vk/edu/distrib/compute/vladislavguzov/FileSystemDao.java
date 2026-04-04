package company.vk.edu.distrib.compute.vladislavguzov;

import company.vk.edu.distrib.compute.Dao;

import java.io.IOException;
import java.nio.file.*;
import java.util.NoSuchElementException;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class FileSystemDao implements Dao<byte[]> {

    private final Path basePath;
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    public FileSystemDao(String basePath) throws IOException {
        this.basePath = Paths.get(basePath).toAbsolutePath().normalize();
        Files.createDirectories(this.basePath);
    }

    @Override
    public byte[] get(String key) throws NoSuchElementException, IllegalArgumentException, IOException {
        validateKey(key);
        Path filePath = resolveFilePath(key);

        lock.readLock().lock();
        try {
            if (!Files.isRegularFile(filePath)) {
                throw new NoSuchElementException("No value found for key: " + key);
            }
            return Files.readAllBytes(filePath);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void upsert(String key, byte[] value) throws IllegalArgumentException, IOException {
        validateKey(key);
        validateValue(key, value);
        Path filePath = resolveFilePath(key);

        lock.writeLock().lock();
        try {
            Path tempFile = Files.createTempFile(basePath, ".tmp_", ".dat");
            try {
                Files.write(tempFile, value, StandardOpenOption.TRUNCATE_EXISTING);
                Files.move(tempFile, filePath, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            } catch (AtomicMoveNotSupportedException e) {
                Files.write(filePath, value, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
                Files.deleteIfExists(tempFile);
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void delete(String key) throws IllegalArgumentException, IOException {
        validateKey(key);
        Path filePath = resolveFilePath(key);

        lock.writeLock().lock();
        try {
            Files.deleteIfExists(filePath);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void close() throws IOException {
        // nothing to close
    }

    private Path resolveFilePath(String key) {
        String safeFileName = key.replaceAll("[/\\\\:*?\"<>|]", "_");
        if (safeFileName.length() > 200) {
            safeFileName = safeFileName.substring(0, 200)
                    + "_"
                    + Integer.toHexString(key.hashCode());
        }
        return basePath.resolve(safeFileName + ".dat");
    }

    private static void validateKey(String key) throws IllegalArgumentException {
        if (key == null || key.isBlank()) {
            throw new IllegalArgumentException("key cannot be null or blank");
        }
        if (key.length() > 1024) {
            throw new IllegalArgumentException("key too long (max 1024 chars)");
        }
    }

    private static void validateValue(String key, byte[] value) throws IllegalArgumentException {
        if (value == null) {
            throw new IllegalArgumentException("value cannot be null for key: " + key);
        }
    }
}
