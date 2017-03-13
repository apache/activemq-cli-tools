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
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.activemq.artemis.utils.SizeFormatterUtil;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.filter.DestinationFilter;
import org.apache.activemq.store.MessageRecoveryListener;
import org.apache.activemq.store.MessageStore;
import org.apache.activemq.store.kahadb.KahaDBPersistenceAdapter;
import org.apache.activemq.util.IOExceptionSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KahaDBExporter implements MessageStoreExporter {

    static final Logger LOG = LoggerFactory.getLogger(KahaDBExporter.class);

    private final KahaDBPersistenceAdapter adapter;
    private final MessageStoreMetadataExporter metadataExporter;
    private final MessageRecoveryListener recoveryListener;

    public KahaDBExporter(final KahaDBPersistenceAdapter adapter,
            final MessageStoreMetadataExporter metadataExporter,
            final MessageRecoveryListener recoveryListener) {
        this.adapter = adapter;
        this.metadataExporter = metadataExporter;
        this.recoveryListener = recoveryListener;
    }

    @Override
    public void exportMetadata() throws IOException {
        metadataExporter.export();
    }

    @Override
    public void exportQueues() throws IOException {
        exportQueues(DestinationFilter.ANY_DESCENDENT);
    }

    @Override
    public void exportTopics() throws IOException {
        exportTopics(DestinationFilter.ANY_DESCENDENT);
    }

    @Override
    public void exportQueues(String pattern) throws IOException {
        pattern = pattern != null ? pattern : DestinationFilter.ANY_DESCENDENT;
        exportDestinations(new ActiveMQQueue(pattern));
    }

    @Override
    public void exportTopics(String pattern) throws IOException {
        pattern = pattern != null ? pattern : DestinationFilter.ANY_DESCENDENT;
        exportDestinations(new ActiveMQTopic(pattern));
    }

    private void exportDestinations(ActiveMQDestination destPattern) throws IOException {

        //Main destination filter
        final DestinationFilter destFilter = DestinationFilter.parseFilter(destPattern);
        //Secondary filter to catch a couple of extra edge cases
        final Predicate<ActiveMQDestination> f = getExportDestinationFilter(destPattern);

        final Set<ActiveMQDestination> destinations = adapter.getDestinations().stream()
                .filter(dest -> destFilter.matches(dest))
                .filter(f)
                .collect(Collectors.toSet());

        // loop through all queues and export them
        for (final ActiveMQDestination destination : destinations) {
            final MessageStore messageStore = destination.isQueue() ?
                    adapter.createQueueMessageStore((ActiveMQQueue) destination) :
                    adapter.createTopicMessageStore((ActiveMQTopic) destination);

            try {
                messageStore.start();

                LOG.info("Starting export of: {}; message count: {} message(s); message size: {}", destination,
                        messageStore.getMessageCount(), SizeFormatterUtil.sizeof(
                                messageStore.getMessageSize()));

                // migrate the data
                messageStore.recover(recoveryListener);
                messageStore.stop();
            } catch (Exception e) {
                IOExceptionSupport.create(e);
            }
        }
    }

    /**
     * Do extra processing to filter out destinations.  This is because the destination filter
     * will match some destianations that we don't want.  For example, "test.queue" will match
     * as true for the pattern "test.queue.>" which is not correct.
     * @param destPattern
     * @return
     */
    private Predicate<ActiveMQDestination> getExportDestinationFilter(final ActiveMQDestination destPattern) {
        //We need to check each composite destination individually
        final List<ActiveMQDestination> nonComposite = destPattern.isComposite()
                ? Arrays.asList(destPattern.getCompositeDestinations()) : Arrays.asList(destPattern);

        return (e) -> {
            boolean match = false;
            for (ActiveMQDestination d : nonComposite) {
                String destString = d.getPhysicalName();
                //don't match a.b when using a.b.>
                if (destPattern.isPattern() && destString.length() > 1 && destString.endsWith(DestinationFilter.ANY_DESCENDENT)) {
                    final String startsWithString = destString.substring(0, destString.length() - 2);
                    match = e.getPhysicalName().startsWith(startsWithString) && !e.getPhysicalName().equals(startsWithString);
                //non wildcard should be an exact match
                } else if (!destPattern.isPattern()) {
                    match = e.getPhysicalName().equals(destString);
                } else {
                    match = true;
                }
                if (match) {
                    break;
                }
            }

            return match;
        };

    }

}
