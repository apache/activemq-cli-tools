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

import org.apache.activemq.cli.artemis.schema.ArtemisJournalMarshaller;
import org.apache.activemq.cli.schema.MessageType;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.store.MessageRecoveryListener;
import org.apache.activemq.store.kahadb.KahaDBStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Recovery listener that can be used to export messages to Artemis
 */
public class ArtemisXmlMessageRecoveryListener implements MessageRecoveryListener {
    static final Logger LOG = LoggerFactory.getLogger(ArtemisXmlMessageRecoveryListener.class);

    private final ArtemisJournalMarshaller xmlMarshaller;
    private final OpenWireCoreMessageTypeConverter converter;

    /**
     * @param file
     */
    public ArtemisXmlMessageRecoveryListener(final KahaDBStore store,
            final ArtemisJournalMarshaller xmlMarshaller) {
        super();
        this.xmlMarshaller = xmlMarshaller;
        this.converter = new OpenWireCoreMessageTypeConverter(store);
    }


    @Override
    public boolean recoverMessage(Message message) throws Exception {
        try {
            MessageType messageType = converter.convert(message);
            xmlMarshaller.appendMessage(messageType);
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            return false;
        }
        return true;
    }


    @Override
    public boolean recoverMessageReference(MessageId ref) throws Exception {
        return false;
    }

    @Override
    public boolean hasSpace() {
        return true;
    }


    @Override
    public boolean isDuplicate(MessageId ref) {
        return false;
    }

}
