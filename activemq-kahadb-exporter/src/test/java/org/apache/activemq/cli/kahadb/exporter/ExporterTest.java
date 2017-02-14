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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
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
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamWriter;

import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.cli.commands.tools.XmlDataImporter;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.config.CoreAddressConfiguration;
import org.apache.activemq.artemis.core.config.CoreQueueConfiguration;
import org.apache.activemq.artemis.core.config.impl.ConfigurationImpl;
import org.apache.activemq.artemis.core.remoting.impl.netty.NettyAcceptorFactory;
import org.apache.activemq.artemis.core.remoting.impl.netty.NettyConnectorFactory;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.impl.ActiveMQServerImpl;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.cli.artemis.schema.ArtemisJournalMarshaller;
import org.apache.activemq.cli.kahadb.exporter.artemis.ArtemisXmlMessageRecoveryListener;
import org.apache.activemq.cli.schema.ActivemqJournalType;
import org.apache.activemq.cli.schema.ObjectFactory;
import org.apache.activemq.cli.schema.QueueBindingType;
import org.apache.activemq.command.ActiveMQBytesMessage;
import org.apache.activemq.command.ActiveMQMapMessage;
import org.apache.activemq.command.ActiveMQObjectMessage;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQStreamMessage;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.store.MessageStore;
import org.apache.activemq.store.kahadb.KahaDBPersistenceAdapter;
import org.apache.activemq.util.ByteSequence;
import org.apache.activemq.util.IdGenerator;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExporterTest {

    static final Logger LOG = LoggerFactory.getLogger(ExporterTest.class);

    @Rule
    public TemporaryFolder storeFolder = new TemporaryFolder();

    /**
     * TODO Improve test when real exporting is done, for now this just
     * tests that the recovery listener iterates over all the queue messages
     *
     * @throws Exception
     */
    @Test
    public void testExportQueues() throws Exception {

        ActiveMQQueue queue = new ActiveMQQueue("test.queue");
        KahaDBPersistenceAdapter adapter = new KahaDBPersistenceAdapter();
        adapter.setJournalMaxFileLength(1024 * 1024);
        adapter.setDirectory(storeFolder.newFolder());
        adapter.start();
        MessageStore messageStore = adapter.createQueueMessageStore(queue);
        messageStore.start();

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
        byte[] bytes = new byte[] {10, 11, 12};
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

        Date date = new Date();
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

        messageStore.stop();

        File file = storeFolder.newFile();
        try(FileOutputStream fos = new FileOutputStream(file)) {
            XMLStreamWriter xmlWriter = XMLOutputFactory.newFactory().createXMLStreamWriter(fos);
            ArtemisJournalMarshaller xmlMarshaller = new ArtemisJournalMarshaller(xmlWriter);

            xmlMarshaller.appendJournalOpen();
            xmlMarshaller.appendBindingsElement();
            xmlMarshaller.appendBinding(QueueBindingType.builder()
                    .withName("test.queue")
                    .withAddress("test.queue").build());
            xmlMarshaller.appendEndElement();
            xmlMarshaller.appendMessagesElement();

            KahaDBExporter dbExporter = new KahaDBExporter(adapter,
                    new ArtemisXmlMessageRecoveryListener(xmlMarshaller));

            dbExporter.exportQueues();
            xmlMarshaller.appendJournalClose(true);
        }


        try (BufferedReader br = new BufferedReader(new FileReader(file))) {
            String line = null;
            while ((line = br.readLine()) != null) {
                System.out.println(line);
            }
         }


        validate(file, 17);

        final ActiveMQServer artemisServer = buildArtemisBroker();
        artemisServer.start();

        XmlDataImporter dataImporter = new XmlDataImporter();
        dataImporter.process(file.getAbsolutePath(), "localhost", 61400, false);

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

        configuration.addAddressConfiguration(new CoreAddressConfiguration()
                .setName("test.queue")
                .addRoutingType(RoutingType.ANYCAST)
                .addQueueConfiguration(new CoreQueueConfiguration()
                        .setAddress("test.queue")
                        .setName("test.queue")
                        .setRoutingType(RoutingType.ANYCAST))
                );

       return new ActiveMQServerImpl(configuration);
    }

    @SuppressWarnings("unchecked")
    private void validate(File file, int count) throws JAXBException {
        JAXBContext jaxbContext = JAXBContext.newInstance(ObjectFactory.class);
        Unmarshaller jaxbUnmarshaller = jaxbContext.createUnmarshaller();
        JAXBElement<ActivemqJournalType> read = (JAXBElement<ActivemqJournalType>) jaxbUnmarshaller.unmarshal(file);
        assertEquals(count, read.getValue().getMessages().getMessage().size());
    }

}
