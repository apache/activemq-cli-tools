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

import java.io.IOException;

import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.jms.client.ActiveMQDestination;
import org.apache.activemq.artemis.utils.CompositeAddress;
import org.apache.activemq.cli.artemis.schema.ArtemisJournalMarshaller;
import org.apache.activemq.cli.kahadb.exporter.ExportConfiguration;
import org.apache.activemq.cli.kahadb.exporter.MessageStoreMetadataExporter;
import org.apache.activemq.cli.schema.QueueBindingType;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.command.SubscriptionInfo;
import org.apache.activemq.store.kahadb.KahaDBStore;

public class ArtemisXmlMetadataExporter implements MessageStoreMetadataExporter {

    private final KahaDBStore store;
    private final ArtemisJournalMarshaller xmlMarshaller;
    private final ExportConfiguration config;


    /**
     * @param xmlMarshaller
     */
    public ArtemisXmlMetadataExporter(final KahaDBStore store,
            final ArtemisJournalMarshaller xmlMarshaller, final ExportConfiguration config) {
        super();
        this.store = store;
        this.xmlMarshaller = xmlMarshaller;
        this.config = config;
    }

    @Override
    public void export() throws IOException {
        store.getDestinations().stream()
        .forEach(dest -> {
            try {
                if (dest.isQueue()) {
                    org.apache.activemq.command.ActiveMQDestination mappedToFQQN = config.mapToDurableSubFQQN(dest);
                    if (dest != mappedToFQQN) {
                        xmlMarshaller.appendBinding(QueueBindingType.builder()
                                                       .withName(CompositeAddress.extractQueueName(mappedToFQQN.getPhysicalName()))
                                                       .withRoutingType(RoutingType.MULTICAST.toString())
                                                       .withAddress(CompositeAddress.extractAddressName(mappedToFQQN.getPhysicalName())).build());
                    } else {
                        xmlMarshaller.appendBinding(QueueBindingType.builder()
                                                       .withName(dest.getPhysicalName())
                                                       .withRoutingType(RoutingType.ANYCAST.toString())
                                                       .withAddress(dest.getPhysicalName()).build());
                    }
                } else if (dest.isTopic()) {
                        for (SubscriptionInfo info :
                            store.createTopicMessageStore((ActiveMQTopic) dest).getAllSubscriptions()) {
                            xmlMarshaller.appendBinding(QueueBindingType.builder()
                                    .withName(ActiveMQDestination.createQueueNameForSubscription(
                                            true, info.getClientId(), info.getSubcriptionName()).toString())
                                    .withRoutingType(RoutingType.MULTICAST.toString())
                                    .withAddress(dest.getPhysicalName()).build());
                        }
                }
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        });

    }

}
