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
import java.util.zip.GZIPOutputStream;

import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamWriter;

import org.apache.activemq.cli.artemis.schema.ArtemisJournalMarshaller;
import org.apache.activemq.cli.kahadb.exporter.artemis.ArtemisXmlMessageRecoveryListener;
import org.apache.activemq.cli.kahadb.exporter.artemis.ArtemisXmlMetadataExporter;
import org.apache.activemq.store.kahadb.KahaDBPersistenceAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * KahaDB Exporter
 */
public class Exporter {

    static final Logger LOG = LoggerFactory.getLogger(Exporter.class);

    public static void main(String[] args) {


    }

    public static void exportKahaDbStore(final File kahaDbDir, final File artemisXml) throws Exception {
        Exporter.exportKahaDbStore(kahaDbDir, artemisXml, false);
    }

    public static void exportKahaDbStore(final File kahaDbDir, final File artemisXml,
            boolean compress) throws Exception {

        KahaDBPersistenceAdapter adapter = new KahaDBPersistenceAdapter();
        adapter.setDirectory(kahaDbDir);
        adapter.start();

        if (artemisXml.exists()) {
            throw new IllegalStateException("File: " + artemisXml + " already exists");
        }

        long start = System.currentTimeMillis();
        try(OutputStream fos = new BufferedOutputStream(compress ? new GZIPOutputStream(
                new FileOutputStream(artemisXml)) : new FileOutputStream(artemisXml))) {

            final XMLStreamWriter xmlWriter = XMLOutputFactory.newFactory().createXMLStreamWriter(fos);
            final ArtemisJournalMarshaller xmlMarshaller = new ArtemisJournalMarshaller(xmlWriter);
            final KahaDBExporter dbExporter = new KahaDBExporter(adapter,
                    new ArtemisXmlMetadataExporter(adapter.getStore(), xmlMarshaller),
                    new ArtemisXmlMessageRecoveryListener(adapter.getStore(), xmlMarshaller));

            xmlMarshaller.appendJournalOpen();
            xmlMarshaller.appendBindingsElement();
            dbExporter.exportMetadata();
            xmlMarshaller.appendEndElement();
            xmlMarshaller.appendMessagesElement();
            dbExporter.exportQueues();
            dbExporter.exportTopics();
            xmlMarshaller.appendEndElement();
            xmlMarshaller.appendJournalClose(true);
        } finally {
            adapter.stop();
        }
        long end = System.currentTimeMillis();

        LOG.info("Total export time: " + (end - start) + " ms");
    }
}
