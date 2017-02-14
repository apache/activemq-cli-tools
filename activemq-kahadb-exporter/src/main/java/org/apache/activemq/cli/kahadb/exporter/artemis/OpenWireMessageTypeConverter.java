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

import javax.jms.JMSException;

import org.apache.activemq.artemis.cli.commands.tools.XmlDataExporterUtil;
import org.apache.activemq.artemis.core.protocol.openwire.OpenWireMessageConverter;
import org.apache.activemq.artemis.core.server.ServerMessage;
import org.apache.activemq.cli.kahadb.exporter.OpenWireExportConverter;
import org.apache.activemq.cli.schema.BodyType;
import org.apache.activemq.cli.schema.MessageType;
import org.apache.activemq.cli.schema.PropertiesType;
import org.apache.activemq.cli.schema.PropertyType;
import org.apache.activemq.cli.schema.QueueType;
import org.apache.activemq.cli.schema.QueuesType;
import org.apache.activemq.command.Message;
import org.apache.activemq.openwire.OpenWireFormat;

public class OpenWireMessageTypeConverter implements OpenWireExportConverter<MessageType> {

    final OpenWireMessageConverter converter = new OpenWireMessageConverter(new OpenWireFormat());

    /* (non-Javadoc)
     * @see org.apache.activemq.cli.kahadb.exporter.MessageConverter#convert(org.apache.activemq.Message)
     */
    @Override
    public MessageType convert(final Message message) throws Exception {
        final ServerMessage serverMessage = converter.inbound(message);
        final MessageType messageType = convertAttributes(serverMessage);

        try {
            if (!message.getProperties().isEmpty()) {
                final PropertiesType propertiesType = new PropertiesType();
                serverMessage.getPropertyNames().forEach(key -> {
                    Object value = serverMessage.getObjectProperty(key);
                    propertiesType.getProperty().add(PropertyType.builder()
                            .withName(key.toString())
                            .withValueAttribute(XmlDataExporterUtil.convertProperty(value))
                            .withType(XmlDataExporterUtil.getPropertyType(value))
                            .build());
                });
                messageType.setProperties(propertiesType);
            }

            messageType.setQueues(convertQueue(message));
            messageType.setBody(convertBody(serverMessage));
        } catch (Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }

        return messageType;
    }

    private QueuesType convertQueue(final Message message) throws JMSException {
        return QueuesType.builder()
                .withQueue(QueueType.builder()
                        .withName(message.getDestination().getPhysicalName()).build())
            .build();
    }

    private BodyType convertBody(final ServerMessage serverMessage) throws Exception {
        String value = XmlDataExporterUtil.encodeMessageBody(serverMessage);

        //requires CDATA
        return BodyType.builder()
            .withValue("<![CDATA[" + value + "]]>")
            .build();
    }

    private MessageType convertAttributes(final ServerMessage message) {
        MessageType messageType = MessageType.builder()
                .withId(message.getMessageID())
                .withTimestamp(message.getTimestamp())
                .withPriority(message.getPriority())
                .withType(XmlDataExporterUtil.getMessagePrettyType(message.getType())).build();

        return messageType;
    }
}
