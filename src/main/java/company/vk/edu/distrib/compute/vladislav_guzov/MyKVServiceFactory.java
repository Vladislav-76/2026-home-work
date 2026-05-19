package company.vk.edu.distrib.compute.vladislav_guzov;

import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.KVServiceFactory;

import java.io.IOException;

public class MyKVServiceFactory extends KVServiceFactory {
    @Override
    protected KVService doCreate(int port) throws IOException {
        return new InMemoryKVService(port, new InMemoryDao());

        // alternative realization
        // return new InMemoryKVService(port, new FileSystemDao("storage"));
    }
}
