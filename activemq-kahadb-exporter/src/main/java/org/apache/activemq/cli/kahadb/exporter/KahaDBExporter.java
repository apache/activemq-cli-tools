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
package org.apache.activemq.cli.kahadb.exporter;

import java.io.IOException;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.command.SubscriptionInfo;
import org.apache.activemq.store.MessageRecoveryListener;
import org.apache.activemq.store.MessageStore;
import org.apache.activemq.store.TopicMessageStore;
import org.apache.activemq.store.kahadb.KahaDBPersistenceAdapter;
import org.apache.activemq.util.IOExceptionSupport;

public class KahaDBExporter implements MessageStoreExporter {

    private final KahaDBPersistenceAdapter adapter;
    private final MessageRecoveryListener recoveryListener;

    public KahaDBExporter (final KahaDBPersistenceAdapter adapter,
            final MessageRecoveryListener recoveryListener) {
        this.adapter = adapter;
        this.recoveryListener = recoveryListener;
    }

    @Override
    public void exportQueues() throws IOException {

        final Set<ActiveMQDestination> destinations = adapter.getDestinations().stream().filter(
                dest -> dest.isQueue()).collect(Collectors.toSet());

        // loop through all queues and export them
        for (final ActiveMQDestination destination : destinations) {

            final ActiveMQQueue queue = (ActiveMQQueue) destination;
            final MessageStore messageStore = adapter.createQueueMessageStore(queue);

            try {
                // migrate the data
                messageStore.recover(recoveryListener);
            } catch (Exception e) {
                IOExceptionSupport.create(e);
            }
        }
    }

    @Override
    public void exportTopics() throws IOException {

        final Set<ActiveMQDestination> destinations = adapter.getDestinations().stream().filter(
                dest -> dest.isTopic()).collect(Collectors.toSet());

        for (ActiveMQDestination destination : destinations) {
            final ActiveMQTopic topic = (ActiveMQTopic) destination;
            final TopicMessageStore messageStore = adapter.createTopicMessageStore(topic);

            //recover subscriptions
            //TODO: This will most likely run into the same message more than once if there is
            //more than one durable sub on a topic so we should look at optimizing this
            //Ideally we'd just recover all the messages once and then ask KahaDB which subscriptions
            //have not acked the message.  This will probably require a new hook into KahaDB
//            for (final SubscriptionInfo subscriptionInfo : messageStore.getAllSubscriptions()) {
//
//                try {
//                    messageStore.recoverSubscription(subscriptionInfo.getClientId(),
//                            subscriptionInfo.getSubscriptionName(), recoveryListener);
//                } catch (Exception e) {
//                    IOExceptionSupport.create(e);
//                }
//            }
        }
    }
}
