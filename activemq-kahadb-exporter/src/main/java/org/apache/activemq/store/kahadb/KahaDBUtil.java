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
package org.apache.activemq.store.kahadb;

import java.util.ArrayList;
import java.util.List;

import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.SubscriptionInfo;
import org.apache.activemq.store.TopicMessageStore;
import org.apache.activemq.store.kahadb.MessageDatabase.LastAck;
import org.apache.activemq.store.kahadb.MessageDatabase.StoredDestination;
import org.apache.activemq.store.kahadb.disk.page.Transaction;

public class KahaDBUtil {


    /**
     * Return subscriptions which have not acked this message
     *
     * @param store
     * @param message
     * @return
     * @throws Exception
     */
    public static List<SubscriptionInfo> getUnackedSubscriptions(KahaDBStore store, Message message)
            throws Exception {

        final List<SubscriptionInfo> matching = new ArrayList<>();

        if (!message.getDestination().isTopic()) {
            return matching;
        }

        ActiveMQTopic topic = (ActiveMQTopic) message.getDestination();
        String messageId = message.getMessageId().toString();
        TopicMessageStore messageStore =  store.createTopicMessageStore(topic);

        store.indexLock.writeLock().lock();

        final SubscriptionInfo[] infos = messageStore.getAllSubscriptions();

        try {
            store.pageFile.tx().execute(new Transaction.Closure<Exception>() {
                @Override
                public void execute(Transaction tx) throws Exception {
                    StoredDestination sd = store.getStoredDestination(store.convert(topic), tx);

                    if (sd != null) {
                        Long position = sd.messageIdIndex.get(tx, messageId);

                        for (SubscriptionInfo info : infos) {
                            LastAck cursorPos = store.getLastAck(tx, sd,
                                    store.subscriptionKey(info.getClientId(), info.getSubcriptionName()));
                            if (cursorPos.lastAckedSequence < position) {
                                matching.add(info);
                            }
                        }
                    }
                }
            });
        } finally {
            store.indexLock.writeLock().unlock();
        }

        return matching;
    }
}
