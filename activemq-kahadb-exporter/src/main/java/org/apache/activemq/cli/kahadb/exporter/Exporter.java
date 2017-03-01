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
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

import org.apache.activemq.cli.artemis.schema.ArtemisJournalMarshaller;
import org.apache.activemq.cli.kahadb.exporter.artemis.ArtemisXmlMessageRecoveryListener;
import org.apache.activemq.cli.kahadb.exporter.artemis.ArtemisXmlMetadataExporter;
import org.apache.activemq.store.PersistenceAdapter;
import org.apache.activemq.store.kahadb.FilteredKahaDBPersistenceAdapter;
import org.apache.activemq.store.kahadb.KahaDBPersistenceAdapter;
import org.apache.activemq.store.kahadb.MultiKahaDBPersistenceAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

/**
 * KahaDB Exporter
 */
public class Exporter {

    static final Logger LOG = LoggerFactory.getLogger(Exporter.class);

    public static void main(String[] args) {


    }

    public static void exportKahaDbStore(final File kahaDbDir, final File artemisXml) throws Exception {
        Exporter.exportStore(kahaDbDir, artemisXml, false, false);
    }

    public static void exportKahaDbStore(final File kahaDbDir, final File artemisXml,
            boolean compress) throws Exception {
        Exporter.exportStore(kahaDbDir, artemisXml, false, compress);
    }

    public static void exportMultiKahaDbStore(final File kahaDbDir, final File artemisXml) throws Exception {
        Exporter.exportStore(kahaDbDir, artemisXml, true, false);
    }

    public static void exportMultiKahaDbStore(final File kahaDbDir, final File artemisXml,
            boolean compress) throws Exception {
        Exporter.exportStore(kahaDbDir, artemisXml, true, compress);
    }

    private static void exportStore(final File kahaDbDir, final File artemisXml,
            boolean multiKaha, boolean compress) throws Exception {

        if (artemisXml.exists()) {
            throw new IllegalStateException("File: " + artemisXml + " already exists");
        }

        long start = System.currentTimeMillis();
        try(OutputStream fos = new BufferedOutputStream(compress ? new GZIPOutputStream(
                new FileOutputStream(artemisXml)) : new FileOutputStream(artemisXml))) {

            final XMLStreamWriter xmlWriter = XMLOutputFactory.newFactory().createXMLStreamWriter(fos);
            final ArtemisJournalMarshaller xmlMarshaller = new ArtemisJournalMarshaller(xmlWriter);

            xmlMarshaller.appendJournalOpen();

            if (multiKaha) {
                appendMultiKahaDbStore(xmlMarshaller, getMultiKahaDbAdapter(kahaDbDir));
            } else {
                appendKahaDbStore(xmlMarshaller, getKahaDbAdapter(kahaDbDir));
            }

            xmlMarshaller.appendJournalClose(true);
        }

        long end = System.currentTimeMillis();

        LOG.info("Total export time: " + (end - start) + " ms");
    }


    private static void appendMultiKahaDbStore(final ArtemisJournalMarshaller xmlMarshaller,
            MultiKahaDBPersistenceAdapter multiAdapter) throws Exception {

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
                dbExporter.exportQueues();
                dbExporter.exportTopics();
            }
            xmlMarshaller.appendEndElement();
        } finally {
            multiAdapter.stop();
        }
    }

    private static void appendKahaDbStore(final ArtemisJournalMarshaller xmlMarshaller,
            KahaDBPersistenceAdapter adapter) throws Exception {

        try {
            adapter.start();

            final KahaDBExporter dbExporter = new KahaDBExporter(adapter,
                    new ArtemisXmlMetadataExporter(adapter.getStore(), xmlMarshaller),
                    new ArtemisXmlMessageRecoveryListener(adapter.getStore(), xmlMarshaller));

            xmlMarshaller.appendBindingsElement();
            dbExporter.exportMetadata();
            xmlMarshaller.appendEndElement();
            xmlMarshaller.appendMessagesElement();
            dbExporter.exportQueues();
            dbExporter.exportTopics();
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
