/**
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
package org.apache.activemq.cli.kahadb.exporter.artemis;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.activemq.artemis.cli.commands.tools.XmlDataConstants;
import org.apache.activemq.artemis.jms.client.ActiveMQDestination;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.cli.schema.MessageType;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.command.SubscriptionInfo;
import org.apache.activemq.store.TopicMessageStore;
import org.apache.activemq.store.kahadb.KahaDBPersistenceAdapter;
import org.apache.activemq.store.kahadb.KahaDBStore;
import org.apache.activemq.util.IdGenerator;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class OpenWireCoreMessageTypeConverterTest {

    @Rule
    public TemporaryFolder storeFolder = new TemporaryFolder();

    //Adapter used for durable subscription conversion to know which messages haven't been acked
    protected KahaDBPersistenceAdapter adapter;
    protected KahaDBStore store;
    protected ConnectionContext context = new ConnectionContext();
    protected IdGenerator id = new IdGenerator();

    @Before
    public void before() throws Exception {
        adapter = new KahaDBPersistenceAdapter();
        adapter.setJournalMaxFileLength(1024 * 1024);
        adapter.setDirectory(storeFolder.getRoot());
        adapter.start();
        store = adapter.getStore();
    }

    @After
    public void after() throws Exception {
        adapter.stop();
    }

    @Test
    public void test() throws Exception {

        ActiveMQTextMessage message = new ActiveMQTextMessage();
        message.setText("test");
        message.setDestination(new ActiveMQQueue("test.queue"));
        message.setMessageId(new MessageId(id.generateId() + ":1", 0));

        OpenWireCoreMessageTypeConverter c = new OpenWireCoreMessageTypeConverter();
        MessageType messageType = c.convert(message);

        assertEquals(XmlDataConstants.TEXT_TYPE_PRETTY, messageType.getType());
        assertEquals("test.queue", messageType.getQueues().getQueue().get(0).getName());
    }


    @Test
    public void testTopicNoStore() throws Exception {

        String topicName = "test.topic";
        ActiveMQTextMessage message = new ActiveMQTextMessage();
        message.setText("test");
        message.setDestination(new ActiveMQTopic(topicName));
        message.setMessageId(new MessageId(id.generateId() + ":1", 0));

        OpenWireCoreMessageTypeConverter c = new OpenWireCoreMessageTypeConverter();
        MessageType messageType = c.convert(message);

        assertEquals(XmlDataConstants.TEXT_TYPE_PRETTY, messageType.getType());
        assertEquals(topicName, messageType.getQueues().getQueue().get(0).getName());
    }

    @Test
    public void testTopicWithStoreNoSubscriptions() throws Exception {

        String topicName = "test.topic";
        ActiveMQTextMessage message = new ActiveMQTextMessage();
        message.setText("test");
        message.setDestination(new ActiveMQTopic(topicName));
        message.setMessageId(new MessageId(id.generateId() + ":1", 0));

        TopicMessageStore ms = adapter.createTopicMessageStore(new ActiveMQTopic(topicName));
        ms.addMessage(context, message);

        OpenWireCoreMessageTypeConverter c = new OpenWireCoreMessageTypeConverter(store);
        MessageType messageType = c.convert(message);

        assertEquals(XmlDataConstants.TEXT_TYPE_PRETTY, messageType.getType());
        assertTrue(messageType.getQueues().getQueue().isEmpty());
    }

    @Test
    public void testTopicWithStoreOneSub() throws Exception {

        String topicName = "test.topic";
        ActiveMQTextMessage message = new ActiveMQTextMessage();
        message.setText("test");
        message.setDestination(new ActiveMQTopic(topicName));
        message.setMessageId(new MessageId(id.generateId() + ":1", 0));

        TopicMessageStore ms = adapter.createTopicMessageStore(new ActiveMQTopic(topicName));
        ms.addSubscription(new SubscriptionInfo("clientId", "subName"), false);
        ms.addMessage(context, message);

        OpenWireCoreMessageTypeConverter c = new OpenWireCoreMessageTypeConverter(store);
        MessageType messageType = c.convert(message);

        assertEquals(XmlDataConstants.TEXT_TYPE_PRETTY, messageType.getType());
        assertEquals(ActiveMQDestination.createQueueNameForDurableSubscription(true, "clientId", "subName"),
                messageType.getQueues().getQueue().get(0).getName());
    }
}
