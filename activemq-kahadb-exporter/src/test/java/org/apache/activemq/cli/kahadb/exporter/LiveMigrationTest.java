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

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.cli.commands.tools.xml.XmlDataImporter;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.config.impl.ConfigurationImpl;
import org.apache.activemq.artemis.core.remoting.impl.netty.NettyAcceptorFactory;
import org.apache.activemq.artemis.core.remoting.impl.netty.NettyConnectorFactory;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.impl.ActiveMQServerImpl;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.cli.kahadb.exporter.ExportConfiguration.ExportConfigurationBuilder;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.apache.activemq.store.PersistenceAdapter;
import org.apache.activemq.store.kahadb.KahaDBPersistenceAdapter;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertTrue;

public class LiveMigrationTest {

   static final Logger LOG = LoggerFactory.getLogger(LiveMigrationTest.class);

   final int numMessagesToSend = 10;
   final CountDownLatch gotAllTest = new CountDownLatch(2*numMessagesToSend);
   final CountDownLatch gotAllPostTest = new CountDownLatch(2*numMessagesToSend);

   @Rule
   public TemporaryFolder storeFolder = new TemporaryFolder();

   public PersistenceAdapter getPersistenceAdapter(File dir) {
      KahaDBPersistenceAdapter adapter = new KahaDBPersistenceAdapter();
      adapter.setJournalMaxFileLength(1024 * 1024);
      adapter.setDirectory(dir);
      return adapter;
   }

   public void exportStore(final ExportConfigurationBuilder builder) throws Exception {
      Exporter.exportStore(builder.build());
   }

   @Test
   public void testMigrateVT() throws Exception {
      File sourceDir = storeFolder.newFolder();
      ActiveMQQueue queueA = new ActiveMQQueue("Consumer.A.VirtualTopic.T");
      ActiveMQQueue queueB = new ActiveMQQueue("Consumer.B.VirtualTopic.T");

      BrokerService classic = startActiveMQClassic(sourceDir);

      int port = classic.getTransportConnectorByScheme("tcp").getConnectUri().getPort();

      // note checkForDuplicates=false is necessary because durable subs messages retain the topic destination, and
      // the classic audit is per destination. Thus two consumers on the same connection (which is unusual) can clash when the audit is enabled
      ConnectionFactory cf = new org.apache.activemq.ActiveMQConnectionFactory("failover:(tcp://localhost:" + port+")?jms.checkForDuplicates=false");
      Connection consumerConnection = cf.createConnection();
      consumerConnection.start();

      final Session sessionA = consumerConnection.createSession(true, Session.SESSION_TRANSACTED);
      MessageConsumer messageConsumerA = sessionA.createConsumer(queueA);
      messageConsumerA.setMessageListener(new MessageListener() {
         @Override
         public void onMessage(Message message) {
            try {
               sessionA.commit();
               tally("A", message);
            } catch (Exception ok) {
               ok.printStackTrace();
            }
         }
      });

      final Session sessionB = consumerConnection.createSession(true, Session.SESSION_TRANSACTED);
      MessageConsumer messageConsumerB = sessionB.createConsumer(queueB);
      messageConsumerB.setMessageListener(new MessageListener() {
         @Override
         public void onMessage(Message message) {
            try {
               sessionB.commit();
               tally("B", message);
            } catch (Exception ok) {
               ok.printStackTrace();
            }
         }
      });

      Connection producerConnection = cf.createConnection();
      try {
         Session producerSession = producerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer messageProducer = producerSession.createProducer(producerSession.createTopic("VirtualTopic.T"));
         for (int i = 0; i < numMessagesToSend; i++) {
            ActiveMQTextMessage message = new ActiveMQTextMessage();
            message.setText("Test: " + i);
            messageProducer.send(message);
         }
      } finally {
         if (producerConnection != null) {
            producerConnection.close();
         }
      }

      TimeUnit.SECONDS.sleep(5);

      assertTrue(gotAllTest.getCount() < 2*numMessagesToSend);

      // stop classic
      classic.stop();

      // migrate and start artemis

      File xmlFile = new File(storeFolder.getRoot().getAbsoluteFile(), "outputXml.xml");
      exportStore(ExportConfigurationBuilder.newBuilder().setSource(sourceDir).setTarget(xmlFile).setVirtualTopicConsumerWildcards("Consumer.*.>;2"));

      printFile(xmlFile);

      final ActiveMQServer artemisServer = buildArtemisBroker(port);
      artemisServer.start();
      artemisServer.getManagementService().enableNotifications(false);

      XmlDataImporter dataImporter = new XmlDataImporter();
      dataImporter.process(xmlFile.getAbsolutePath(), "localhost", port, false);

      try {
         // wait for all messages to be consumed from classic clients
         assertTrue("got all", gotAllTest.await(numMessagesToSend, TimeUnit.SECONDS));

         // lets send some more to be sure to be sure
         producerConnection = cf.createConnection();
         try {
            Session producerSession = producerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageProducer messageProducer = producerSession.createProducer(producerSession.createTopic("VirtualTopic.T"));
            for (int i = 0; i < numMessagesToSend; i++) {
               ActiveMQTextMessage message = new ActiveMQTextMessage();
               message.setText("PostTest: " + i);
               messageProducer.send(message);
            }
         } finally {
            if (producerConnection != null) {
               producerConnection.close();
            }
         }

         assertTrue("got all post test", gotAllPostTest.await(numMessagesToSend, TimeUnit.SECONDS));

      } finally {

         consumerConnection.close();
         artemisServer.stop();
      }
   }

   private void tally(String id, Message message) throws Exception {
      final String text = ((TextMessage) message).getText();
      LOG.info("{} got: {} text val {}", id, message.getJMSMessageID(), text);
      if (text.startsWith("PostTest")) {
         gotAllPostTest.countDown();
      } else {
         gotAllTest.countDown();
         TimeUnit.SECONDS.sleep(1);
      }
   }

   private BrokerService startActiveMQClassic(File sourceDir) throws Exception {
      BrokerService brokerService = new BrokerService();
      brokerService.setPersistenceAdapter(getPersistenceAdapter(sourceDir));
      brokerService.addConnector("tcp://localhost:0");
      brokerService.start();
      return brokerService;
   }

   public ActiveMQServer buildArtemisBroker(int port) throws IOException {
      Configuration configuration = new ConfigurationImpl();

      configuration.setPersistenceEnabled(true);
      configuration.setSecurityEnabled(false);

      Map<String, Object> connectionParams = new HashMap<String, Object>();
      connectionParams.put(
         org.apache.activemq.artemis.core.remoting.impl.netty.TransportConstants.PORT_PROP_NAME, port);
      connectionParams.put("virtualTopicConsumerWildcards","Consumer.*.>;2");


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


   protected void printFile(File file) throws IOException {
      try (BufferedReader br = new BufferedReader(new FileReader(file))) {
         String line = null;
         while ((line = br.readLine()) != null) {
            System.out.println(line);
         }
      }
   }

}
