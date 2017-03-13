/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.cli.kahadb.exporter;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.MapMessage;
import javax.jms.MessageConsumer;
import javax.jms.ObjectMessage;
import javax.jms.Session;
import javax.jms.StreamMessage;
import javax.jms.TextMessage;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;

import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.cli.commands.tools.XmlDataImporter;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.config.impl.ConfigurationImpl;
import org.apache.activemq.artemis.core.remoting.impl.netty.NettyAcceptorFactory;
import org.apache.activemq.artemis.core.remoting.impl.netty.NettyConnectorFactory;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.impl.ActiveMQServerImpl;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.cli.kahadb.exporter.ExportConfiguration.ExportConfigurationBuilder;
import org.apache.activemq.cli.schema.ActivemqJournalType;
import org.apache.activemq.cli.schema.ObjectFactory;
import org.apache.activemq.command.ActiveMQBytesMessage;
import org.apache.activemq.command.ActiveMQMapMessage;
import org.apache.activemq.command.ActiveMQObjectMessage;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQStreamMessage;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.command.SubscriptionInfo;
import org.apache.activemq.store.MessageStore;
import org.apache.activemq.store.PersistenceAdapter;
import org.apache.activemq.store.TopicMessageStore;
import org.apache.activemq.util.ByteSequence;
import org.apache.activemq.util.IdGenerator;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class ExporterTest {

    static final Logger LOG = LoggerFactory.getLogger(ExporterTest.class);

    @Rule
    public TemporaryFolder storeFolder = new TemporaryFolder();

    public abstract PersistenceAdapter getPersistenceAdapter(File dir);

    public abstract void exportStore(final ExportConfigurationBuilder builder) throws Exception;

    @Test
    public void testExportQueuesPattern() throws Exception {
        testExportQueues("test.>");
    }

    /**
     *
     * @throws Exception
     */
    @Test
    public void testExportQueuesAll() throws Exception {
        testExportQueues(null);
    }

    @Test
    public void testExportQueuesPatternEmpty() throws Exception {
        File sourceDir = storeFolder.newFolder();
        ActiveMQQueue queue = new ActiveMQQueue("test.queue");
        PersistenceAdapter adapter = getPersistenceAdapter(sourceDir);
        adapter.start();
        MessageStore messageStore = adapter.createQueueMessageStore(queue);
        messageStore.start();
        publishQueueMessages(messageStore, queue, new Date(), new byte[] {10, 11, 12});
        adapter.stop();

        File xmlFile = new File(storeFolder.getRoot().getAbsoluteFile(), "outputXml.xml");
        exportStore(ExportConfigurationBuilder.newBuilder()
                .setSource(sourceDir)
                .setTarget(xmlFile)
                .setQueuePattern("empty.>"));

        validate(xmlFile, 0);
    }


    protected void testExportQueues(String pattern) throws Exception {

        File kahaDbDir = storeFolder.newFolder();
        ActiveMQQueue queue = new ActiveMQQueue("test.queue");
        PersistenceAdapter adapter = getPersistenceAdapter(kahaDbDir);
        adapter.start();
        MessageStore messageStore = adapter.createQueueMessageStore(queue);
        messageStore.start();

        final byte[] bytes = new byte[] {10, 11, 12};
        final Date date = new Date();
        publishQueueMessages(messageStore, queue, date, bytes);

        adapter.stop();

        File xmlFile = new File(storeFolder.getRoot().getAbsoluteFile(), "outputXml.xml");
        exportStore(ExportConfigurationBuilder.newBuilder()
                .setSource(kahaDbDir)
                .setTarget(xmlFile)
                .setQueuePattern(pattern));

        validate(xmlFile, 17);

        final ActiveMQServer artemisServer = buildArtemisBroker();
        artemisServer.start();

        XmlDataImporter dataImporter = new XmlDataImporter();
        dataImporter.process(xmlFile.getAbsolutePath(), "localhost", 61400, false);

        ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory("tcp://localhost:61400");

        Connection connection = null;
        try {

            connection = cf.createConnection();
            connection.start();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageConsumer messageConsumer = session.createConsumer(session.createQueue("test.queue"));

            for (int i = 0; i < 5; i++) {
                TextMessage messageReceived = (TextMessage) messageConsumer.receive(1000);
                assertNotNull(messageReceived);
                assertEquals("abc", messageReceived.getStringProperty("MyStringProperty"));
                assertEquals("Test", messageReceived.getText());
            }

            for (int i = 0; i < 3; i++) {
                BytesMessage messageReceived = (BytesMessage) messageConsumer.receive(1000);
                assertNotNull(messageReceived);
                assertEquals("abc", messageReceived.getStringProperty("MyStringProperty"));
                assertEquals((byte)10, messageReceived.getByteProperty("MyByteProperty"));
                byte[] result = new byte[3];
                messageReceived.readBytes(result);
                assertArrayEquals(bytes, result);
            }

            for (int i = 0; i < 3; i++) {
                MapMessage messageReceived = (MapMessage) messageConsumer.receive(1000);
                assertNotNull(messageReceived);
                assertEquals("abc", messageReceived.getStringProperty("MyStringProperty"));
                assertEquals("value", messageReceived.getObject("key"));
            }

            for (int i = 0; i < 3; i++) {
                ObjectMessage messageReceived = (ObjectMessage) messageConsumer.receive(1000);
                assertNotNull(messageReceived);
                assertEquals(date, messageReceived.getObject());
            }

            for (int i = 0; i < 3; i++) {
                StreamMessage messageReceived = (StreamMessage) messageConsumer.receive(1000);
                assertNotNull(messageReceived);
                assertEquals((byte)10, messageReceived.readByte());
            }

        } finally {
            if (connection != null) {
                connection.close();
            }
            cf.close();
        }

        artemisServer.stop();
    }

    @Test
    public void testExportTopicsPatternEmpty() throws Exception {
        File kahaDbDir = storeFolder.newFolder();

        ActiveMQTopic topic = new ActiveMQTopic("test.topic");
        PersistenceAdapter adapter = getPersistenceAdapter(kahaDbDir);
        adapter.start();
        TopicMessageStore messageStore = adapter.createTopicMessageStore(topic);
        messageStore.start();

        SubscriptionInfo sub1 = new SubscriptionInfo("clientId1", "sub1");
        sub1.setDestination(topic);
        messageStore.addSubscription(sub1, false);

        IdGenerator id = new IdGenerator();
        ConnectionContext context = new ConnectionContext();
        for (int i = 0; i < 5; i++) {
            ActiveMQTextMessage message = new ActiveMQTextMessage();
            message.setText("Test");
            message.setProperty("MyStringProperty", "abc");
            message.setProperty("MyIntegerProperty", 1);
            message.setDestination(topic);
            message.setMessageId(new MessageId(id.generateId() + ":1", i));
            messageStore.addMessage(context, message);
        }

        adapter.stop();

        //should be empty as no messages match empty.>
        File xmlFile = new File(storeFolder.getRoot().getAbsoluteFile(), "outputXml.xml");
        exportStore(ExportConfigurationBuilder.newBuilder()
                .setTopicPattern("empty.>")
                .setSource(kahaDbDir)
                .setTarget(xmlFile));
        validate(xmlFile, 0);
    }

    @Test
    public void testExportTopicsAll() throws Exception {
        testExportTopics(null);
    }

    @Test
    public void testExportTopicsPattern() throws Exception {
        testExportTopics("test.>");
    }

    protected void testExportTopics(String pattern) throws Exception {

        File kahaDbDir = storeFolder.newFolder();

        ActiveMQTopic topic = new ActiveMQTopic("test.topic");
        PersistenceAdapter adapter = getPersistenceAdapter(kahaDbDir);
        adapter.start();
        TopicMessageStore messageStore = adapter.createTopicMessageStore(topic);
        messageStore.start();

        SubscriptionInfo sub1 = new SubscriptionInfo("clientId1", "sub1");
        SubscriptionInfo sub2 = new SubscriptionInfo("clientId1", "sub2");
        sub1.setDestination(topic);
        messageStore.addSubscription(sub1, false);
        messageStore.addSubscription(sub2, false);

        IdGenerator id = new IdGenerator();
        ConnectionContext context = new ConnectionContext();
        MessageId first = null;
        for (int i = 0; i < 5; i++) {
            ActiveMQTextMessage message = new ActiveMQTextMessage();
            message.setText("Test");
            message.setProperty("MyStringProperty", "abc");
            message.setProperty("MyIntegerProperty", 1);
            message.setDestination(topic);
            message.setMessageId(new MessageId(id.generateId() + ":1", i));
            messageStore.addMessage(context, message);
            if (i == 0) {
                first = message.getMessageId();
            }
        }

        //ack for sub1 only
        messageStore.acknowledge(context, "clientId1", "sub1", first, new MessageAck());

        adapter.stop();

        File xmlFile = new File(storeFolder.getRoot().getAbsoluteFile(), "outputXml.xml");
        exportStore(ExportConfigurationBuilder.newBuilder()
                .setTopicPattern(pattern)
                .setSource(kahaDbDir)
                .setTarget(xmlFile));

       // printFile(xmlFile);

        validate(xmlFile, 5);

        final ActiveMQServer artemisServer = buildArtemisBroker();
        artemisServer.start();

        XmlDataImporter dataImporter = new XmlDataImporter();
        dataImporter.process(xmlFile.getAbsolutePath(), "localhost", 61400, false);

        ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory("tcp://localhost:61400");

        Connection connection = null;
        try {

            connection = cf.createConnection();
            connection.setClientID("clientId1");
            connection.start();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageConsumer messageConsumer = session.createSharedDurableConsumer(
                    session.createTopic("test.topic"), "sub1");
            MessageConsumer messageConsumer2 = session.createSharedDurableConsumer(
                    session.createTopic("test.topic"), "sub2");

            for (int i = 0; i < 5; i++) {
                TextMessage messageReceived1 = (TextMessage) messageConsumer.receive(1000);
                if (i < 4) {
                    assertNotNull(messageReceived1);
                } else {
                    assertNull(messageReceived1);
                }
                TextMessage messageReceived2 = (TextMessage) messageConsumer2.receive(1000);
                assertNotNull(messageReceived2);

                assertEquals("abc", messageReceived2.getStringProperty("MyStringProperty"));
                assertEquals("Test", messageReceived2.getText());
            }

        } finally {
            if (connection != null) {
                connection.close();
            }
            cf.close();
        }

        artemisServer.stop();
    }

    public ActiveMQServer buildArtemisBroker() throws IOException {
        Configuration configuration = new ConfigurationImpl();

        configuration.setPersistenceEnabled(true);
        configuration.setSecurityEnabled(false);

        Map<String, Object> connectionParams = new HashMap<String, Object>();
        connectionParams.put(
                org.apache.activemq.artemis.core.remoting.impl.netty.TransportConstants.PORT_PROP_NAME, 61400);

        configuration.setBindingsDirectory(storeFolder.newFolder().getAbsolutePath());
        configuration.setJournalDirectory(storeFolder.newFolder().getAbsolutePath());
        configuration.setLargeMessagesDirectory(storeFolder.newFolder().getAbsolutePath());
        configuration.setPagingDirectory(storeFolder.newFolder().getAbsolutePath());

        configuration.addAcceptorConfiguration(
                new TransportConfiguration(NettyAcceptorFactory.class.getName(), connectionParams));
        configuration.addConnectorConfiguration("connector",
                new TransportConfiguration(NettyConnectorFactory.class.getName(), connectionParams));


       return new ActiveMQServerImpl(configuration);
    }

    private void publishQueueMessages(MessageStore messageStore, ActiveMQQueue queue,
            Date date, byte[] bytes) throws Exception {
        IdGenerator id = new IdGenerator();
        ConnectionContext context = new ConnectionContext();
        for (int i = 0; i < 5; i++) {
            ActiveMQTextMessage message = new ActiveMQTextMessage();
            message.setText("Test");
            message.setProperty("MyStringProperty", "abc");
            message.setProperty("MyIntegerProperty", 1);
            message.setDestination(queue);
            message.setMessageId(new MessageId(id.generateId() + ":1", i));
            messageStore.addMessage(context, message);
        }

        for (int i = 0; i < 3; i++) {
            ActiveMQBytesMessage message = new ActiveMQBytesMessage();

            message.setContent(new ByteSequence(bytes));
            message.setProperty("MyStringProperty", "abc");
            message.setProperty("MyByteProperty", (byte)10);
            message.setDestination(queue);
            message.setMessageId(new MessageId(id.generateId() + ":2", i));
            messageStore.addMessage(context, message);
        }

        for (int i = 0; i < 3; i++) {
            ActiveMQMapMessage message = new ActiveMQMapMessage();
            message.setObject("key", "value");
            message.setObject("key2", 10);
            message.setProperty("MyStringProperty", "abc");
            message.setDestination(queue);
            message.setMessageId(new MessageId(id.generateId() + ":3", i));
            messageStore.addMessage(context, message);
        }


        for (int i = 0; i < 3; i++) {
            ActiveMQObjectMessage message = new ActiveMQObjectMessage();
            message.setObject(date);
            message.setDestination(queue);
            message.setMessageId(new MessageId(id.generateId() + ":4", i));
            messageStore.addMessage(context, message);
        }

        for (int i = 0; i < 3; i++) {
            ActiveMQStreamMessage message = new ActiveMQStreamMessage();
            message.writeByte((byte)10);
            message.storeContentAndClear();
            message.setDestination(queue);
            message.setMessageId(new MessageId(id.generateId() + ":5", i));
            messageStore.addMessage(context, message);
        }
    }

    @SuppressWarnings("unchecked")
    private void validate(File file, int count) throws JAXBException {
        JAXBContext jaxbContext = JAXBContext.newInstance(ObjectFactory.class);
        Unmarshaller jaxbUnmarshaller = jaxbContext.createUnmarshaller();
        JAXBElement<ActivemqJournalType> read = (JAXBElement<ActivemqJournalType>) jaxbUnmarshaller.unmarshal(file);
        assertEquals(count, read.getValue().getMessages().getMessage().size());
    }

    protected void printFile(File file) throws IOException {
        try (BufferedReader br = new BufferedReader(new FileReader(file))) {
            String line = null;
            while ((line = br.readLine()) != null) {
                System.out.println(line);
            }
         }
    }

}
