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

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.stream.Collectors;
import java.util.zip.GZIPOutputStream;

import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamWriter;

import org.apache.activemq.cli.artemis.schema.ArtemisJournalMarshaller;
import org.apache.activemq.cli.kahadb.exporter.ExportConfiguration.ExportConfigurationBuilder;
import org.apache.activemq.cli.kahadb.exporter.artemis.ArtemisXmlMessageRecoveryListener;
import org.apache.activemq.cli.kahadb.exporter.artemis.ArtemisXmlMetadataExporter;
import org.apache.activemq.store.kahadb.FilteredKahaDBPersistenceAdapter;
import org.apache.activemq.store.kahadb.KahaDBPersistenceAdapter;
import org.apache.activemq.store.kahadb.MultiKahaDBPersistenceAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import io.airlift.airline.Cli;
import io.airlift.airline.Cli.CliBuilder;
import io.airlift.airline.Command;
import io.airlift.airline.Help;
import io.airlift.airline.Option;
import io.airlift.airline.OptionType;

/**
 * KahaDB Exporter
 */
public class Exporter {

    static final Logger LOG = LoggerFactory.getLogger(Exporter.class);

    @SuppressWarnings("unchecked")
    public static void main(String[] args) {

        CliBuilder<Runnable> builder = Cli.<Runnable>builder("export")
                .withDescription("Export a KahaDB or MultiKahaDB store to Artemis XML")
                .withDefaultCommand(Help.class)
                .withCommands(Help.class, ExportKahaDb.class, ExportMultiKahaDb.class);

        Cli<Runnable> gitParser = builder.build();

        gitParser.parse(args).run();

    }

    @Command(name = "kahadb", description = "Export a KahaDb store to Artemis XML")
    public static class ExportKahaDb implements Runnable
    {
        @Option(name = {"-s", "--source"}, required = true, type = OptionType.COMMAND, description = "Data store directory location")
        public String source;

        @Option(name = {"-t", "--target"}, required = true, type = OptionType.COMMAND, description = "Xml output file location")
        public String target;

        @Option(name = {"--qp", "--queuePattern"}, type = OptionType.COMMAND, description = "Queue Export Pattern")
        public String queuePattern;

        @Option(name = {"--tp", "--topicPattern"}, type = OptionType.COMMAND, description = "Topic Export Pattern")
        public String topicPattern;

        @Option(name = "-c", type = OptionType.COMMAND, description = "Compress output xml file using gzip")
        public boolean compress;

        @Option(name = "-f", type = OptionType.COMMAND, description = "Force XML output and overwrite existing file")
        public boolean overwrite;

        /* (non-Javadoc)
         * @see java.lang.Runnable#run()
         */
        @Override
        public void run() {
            LOG.info("Starting KahaDB store export");
            try {
                Exporter.exportStore(ExportConfigurationBuilder.newBuilder()
                        .setSource(new File(source))
                        .setTarget(new File(target))
                        .setQueuePattern(queuePattern)
                        .setTopicPattern(topicPattern)
                        .setCompress(compress)
                        .setOverwrite(overwrite)
                        .build());
            } catch (Exception e) {
                LOG.error(e.getMessage(), e);
                throw new IllegalStateException(e.getMessage(), e);
            }

        }
    }

    @Command(name = "mkahadb", description = "Export a MultiKahaDb store to Artemis XML")
    public static class ExportMultiKahaDb extends ExportKahaDb
    {

        /* (non-Javadoc)
         * @see java.lang.Runnable#run()
         */
        @Override
        public void run() {
            LOG.info("Starting MultiKahaDB store export");
            try {
                Exporter.exportStore(ExportConfigurationBuilder.newBuilder()
                        .setMultiKaha(true)
                        .setSource(new File(source))
                        .setTarget(new File(target))
                        .setQueuePattern(queuePattern)
                        .setTopicPattern(topicPattern)
                        .setCompress(compress)
                        .setOverwrite(overwrite)
                        .build());
            } catch (Exception e) {
                LOG.error(e.getMessage(), e);
                throw new IllegalStateException(e.getMessage(), e);
            }

        }
    }

    public static void exportStore(final ExportConfiguration config) throws Exception {

        if (!config.isOverwrite() && config.getTarget().exists()) {
            throw new IllegalStateException("File: " + config.getTarget() + " already exists");
        }

        long start = System.currentTimeMillis();
        try(OutputStream fos = new BufferedOutputStream(config.isCompress() ? new GZIPOutputStream(
                new FileOutputStream(config.getTarget())) : new FileOutputStream(config.getTarget()))) {

            final XMLStreamWriter xmlWriter = XMLOutputFactory.newFactory().createXMLStreamWriter(fos);
            final ArtemisJournalMarshaller xmlMarshaller = new ArtemisJournalMarshaller(xmlWriter);

            xmlMarshaller.appendJournalOpen();

            if (config.isMultiKaha()) {
                appendMultiKahaDbStore(xmlMarshaller, getMultiKahaDbAdapter(config.getSource()),
                        config.getQueuePattern(), config.getTopicPattern());
            } else {
                appendKahaDbStore(xmlMarshaller, getKahaDbAdapter(config.getSource()),
                        config.getQueuePattern(), config.getTopicPattern());
            }

            xmlMarshaller.appendJournalClose(true);
        }

        long end = System.currentTimeMillis();

        LOG.info("Total export time: " + (end - start) + " ms");
    }


    private static void appendMultiKahaDbStore(final ArtemisJournalMarshaller xmlMarshaller,
            final MultiKahaDBPersistenceAdapter multiAdapter, final String queuePattern,
            final String topicPattern) throws Exception {

        try {
            multiAdapter.start();

            List<KahaDBExporter> dbExporters = multiAdapter.getAdapters().stream()
                    .filter(adapter -> adapter instanceof KahaDBPersistenceAdapter)
                    .map(adapter -> {
                        KahaDBPersistenceAdapter kahaAdapter = (KahaDBPersistenceAdapter) adapter;
                        return new KahaDBExporter(kahaAdapter,
                              new ArtemisXmlMetadataExporter(kahaAdapter.getStore(), xmlMarshaller),
                              new ArtemisXmlMessageRecoveryListener(kahaAdapter.getStore(), xmlMarshaller));
            }).collect(Collectors.toList());

            xmlMarshaller.appendBindingsElement();
            for (KahaDBExporter dbExporter : dbExporters) {
                dbExporter.exportMetadata();
            }
            xmlMarshaller.appendEndElement();

            xmlMarshaller.appendMessagesElement();
            for (KahaDBExporter dbExporter : dbExporters) {
                dbExporter.exportQueues(queuePattern);
                dbExporter.exportTopics(topicPattern);
            }
            xmlMarshaller.appendEndElement();
        } finally {
            multiAdapter.stop();
        }
    }

    private static void appendKahaDbStore(final ArtemisJournalMarshaller xmlMarshaller,
            final KahaDBPersistenceAdapter adapter, final String queuePattern, final String topicPattern) throws Exception {

        try {
            adapter.start();

            final KahaDBExporter dbExporter = new KahaDBExporter(adapter,
                    new ArtemisXmlMetadataExporter(adapter.getStore(), xmlMarshaller),
                    new ArtemisXmlMessageRecoveryListener(adapter.getStore(), xmlMarshaller));

            xmlMarshaller.appendBindingsElement();
            dbExporter.exportMetadata();
            xmlMarshaller.appendEndElement();
            xmlMarshaller.appendMessagesElement();
            dbExporter.exportQueues(queuePattern);
            dbExporter.exportTopics(topicPattern);
            xmlMarshaller.appendEndElement();
        } finally {
            adapter.stop();
        }
    }

    private static KahaDBPersistenceAdapter getKahaDbAdapter(File dir) {
        KahaDBPersistenceAdapter adapter = new KahaDBPersistenceAdapter();
        adapter.setDirectory(dir);
        return adapter;
    }

    private static MultiKahaDBPersistenceAdapter getMultiKahaDbAdapter(File dir) {
        MultiKahaDBPersistenceAdapter adapter = new MultiKahaDBPersistenceAdapter();
        adapter.setDirectory(dir);

        KahaDBPersistenceAdapter kahaStore = new KahaDBPersistenceAdapter();
        kahaStore.setDirectory(dir);
        FilteredKahaDBPersistenceAdapter filtered = new FilteredKahaDBPersistenceAdapter();
        filtered.setPersistenceAdapter(kahaStore);
        filtered.setPerDestination(true);

        adapter.setFilteredPersistenceAdapters(Lists.newArrayList(filtered));
        return adapter;
    }
}
