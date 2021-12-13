package com.minimal.reproducer;

import io.quarkus.runtime.Startup;
import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.config.impl.ConfigurationImpl;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.embedded.EmbeddedActiveMQ;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.enterprise.context.ApplicationScoped;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

@Startup
@ApplicationScoped
public class EmbeddedActiveMQServer {
    private static final Logger LOGGER = LoggerFactory.getLogger(EmbeddedActiveMQServer.class);

    private Path directory;
    private EmbeddedActiveMQ embeddedActiveMQ;

    @PostConstruct
    public void setupActiveMQ() throws Exception {
        directory = Files.createDirectories(Files.createTempDirectory("activemq"));

        LOGGER.info("ActiveMQ dir: {}", directory);

        Map<String, Object> properties = new HashMap<>();
        properties.put("protocols", "AMQP");
        properties.put("amqpMinLargeMessageSize", "1048576"); // 1mb - everything above that is written into the large messages folder
        properties.put("compressLargeMessages", false); // usually false, we already compress the data ourselves
        properties.put("jms.copyMessageOnSend", false); // JMS spec requires a copy on send, but we are not modifying messages after sending to we can turn it off
        properties.put("jms.prefetchPolicy.all", 1000); // default 1000
        properties.put("jms.redeliveryPolicy.maximumRedeliveries", -1); // -1 => unlimited redeliveries
        properties.put("jms.useCompression", false); // usually false, we already compress the data ourselves
        properties.put("jms.optimizeAcknowledge", true); // if true then ActiveMQ acknowledges messages in batches
        properties.put("jms.useAsyncSend", true);

        String connectionProperties = properties.entrySet().stream()
                .map(e -> e.getKey() + "=" + e.getValue())
                .collect(Collectors.joining("&"));

        LOGGER.info("ActiveMQ connection properties: {}", connectionProperties);

        Configuration config = new ConfigurationImpl();
        config.addAcceptorConfiguration("amqp", "tcp://127.0.0.1:5672?" + connectionProperties);

        config.setSecurityEnabled(false);
        config.setPersistenceEnabled(true);

        config.setBindingsDirectory(directory.resolve("bindings").toString());
        config.setCreateBindingsDir(true);

        config.setJournalDirectory(directory.resolve("journal").toString());
        config.setCreateJournalDir(true);

        config.setLargeMessagesDirectory(directory.resolve("largeMessages").toString());
        config.setPagingDirectory(directory.resolve("paging").toString());

        embeddedActiveMQ = new EmbeddedActiveMQ().setConfiguration(config);

        LOGGER.info("Starting embedded ActiveMQ");
        ActiveMQServer activeMQServer = embeddedActiveMQ.start().getActiveMQServer();
        activeMQServer.createQueue(newQueue("queue-A"), true);
        activeMQServer.createQueue(newQueue("queue-B"), true);
        activeMQServer.createQueue(newQueue("queue-C"), true);
        LOGGER.info("Embedded ActiveMQ started");

        // just some cleanup to that I'm not polluting your temp folder too much
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                embeddedActiveMQ.stop();
                FileUtils.deleteDirectory(directory.toFile());
            } catch (Exception e) {
                LOGGER.info("", e);
            }
        }));
    }

    private static QueueConfiguration newQueue(String name) {
        QueueConfiguration queueConfiguration = new QueueConfiguration(name);
        queueConfiguration.setDurable(true); // persist messages to disk
        queueConfiguration.setMaxConsumers(1); // only one consumer
        queueConfiguration.setRoutingType(RoutingType.ANYCAST); // makes it a queue
        queueConfiguration.setAutoDelete(false); // do not delete the queue when empty
        queueConfiguration.setPurgeOnNoConsumers(false); // keep messages even when nobody is consuming them
        return queueConfiguration;
    }

    @PreDestroy
    public void shutdownActiveMQ() throws Exception {
        if (embeddedActiveMQ != null) {
            LOGGER.info("Stopping embedded ActiveMQ");
            embeddedActiveMQ.stop();
            LOGGER.info("Embedded ActiveMQ stopped");
        }
    }



}
