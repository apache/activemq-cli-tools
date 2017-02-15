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

import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileOutputStream;

import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamWriter;

import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.jms.client.ActiveMQDestination;
import org.apache.activemq.cli.artemis.schema.ArtemisJournalMarshaller;
import org.apache.activemq.cli.kahadb.exporter.artemis.ArtemisXmlMessageRecoveryListener;
import org.apache.activemq.cli.schema.QueueBindingType;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.command.SubscriptionInfo;
import org.apache.activemq.store.kahadb.KahaDBPersistenceAdapter;

/**
 * KahaDB Exporter
 */
public class Exporter {

    public static void main(String[] args) {

    }

    public static void exportKahaDbStore(final File kahaDbDir, final File artemisXml) throws Exception {

        KahaDBPersistenceAdapter adapter = new KahaDBPersistenceAdapter();
        adapter.setDirectory(kahaDbDir);
        adapter.start();

        try(FileOutputStream fos = new FileOutputStream(artemisXml)) {
            XMLStreamWriter xmlWriter = XMLOutputFactory.newFactory().createXMLStreamWriter(fos);
            ArtemisJournalMarshaller xmlMarshaller = new ArtemisJournalMarshaller(xmlWriter);

            xmlMarshaller.appendJournalOpen();
            xmlMarshaller.appendBindingsElement();

            adapter.getStore().getDestinations().stream()
                .forEach(dest -> {
                    try {
                        if (dest.isQueue()) {
                            xmlMarshaller.appendBinding(QueueBindingType.builder()
                                    .withName(dest.getPhysicalName())
                                    .withRoutingType(RoutingType.ANYCAST.toString())
                                    .withAddress(dest.getPhysicalName()).build());
                        } else if (dest.isTopic()) {
                                for (SubscriptionInfo info :
                                    adapter.getStore().createTopicMessageStore((ActiveMQTopic) dest).getAllSubscriptions()) {
                                    xmlMarshaller.appendBinding(QueueBindingType.builder()
                                            .withName(ActiveMQDestination.createQueueNameForDurableSubscription(
                                                    true, info.getClientId(), info.getSubcriptionName()))
                                            .withRoutingType(RoutingType.MULTICAST.toString())
                                            .withAddress(dest.getPhysicalName()).build());
                                }
                        }
                    } catch (Exception e) {
                        throw new IllegalStateException(e);
                    }
                });

            xmlMarshaller.appendEndElement();
            xmlMarshaller.appendMessagesElement();

            KahaDBExporter dbExporter = new KahaDBExporter(adapter,
                    new ArtemisXmlMessageRecoveryListener(adapter.getStore(), xmlMarshaller));

            dbExporter.exportQueues();
            dbExporter.exportTopics();
            xmlMarshaller.appendJournalClose(true);
        } finally {
            adapter.stop();
        }
    }
}
