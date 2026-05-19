package company.vk.edu.distrib.compute.vladislav_guzov;

import company.vk.edu.distrib.compute.AuditService;

public class MyAuditServiceFactory extends company.vk.edu.distrib.compute.AuditServiceFactory {
    @Override
    protected AuditService doCreate(String bootstrapServers, String consumerGroupId) {
        return new AuditServiceImpl(bootstrapServers, consumerGroupId);
    }
}
