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

import java.io.File;

import org.apache.activemq.cli.kahadb.exporter.ExportConfiguration.ExportConfigurationBuilder;
import org.apache.activemq.store.PersistenceAdapter;
import org.apache.activemq.store.kahadb.KahaDBPersistenceAdapter;

public class KahaDbExporterTest extends ExporterTest {

    @Override
    public PersistenceAdapter getPersistenceAdapter(File dir) {
        KahaDBPersistenceAdapter adapter = new KahaDBPersistenceAdapter();
        adapter.setJournalMaxFileLength(1024 * 1024);
        adapter.setDirectory(dir);
        return adapter;
    }

    @Override
    public void exportStore(final ExportConfigurationBuilder builder) throws Exception {
        Exporter.exportStore(builder.build());
    }
}
