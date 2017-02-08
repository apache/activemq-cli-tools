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

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.protocol.openwire.OpenWireMessageConverter;
import org.apache.activemq.artemis.core.server.ServerMessage;
import org.apache.activemq.artemis.utils.Base64;
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

    static final String MESSAGE_TIMESTAMP = "timestamp";
    static final String DEFAULT_TYPE_PRETTY = "default";
    static final String BYTES_TYPE_PRETTY = "bytes";
    static final String MAP_TYPE_PRETTY = "map";
    static final String OBJECT_TYPE_PRETTY = "object";
    static final String STREAM_TYPE_PRETTY = "stream";
    static final String TEXT_TYPE_PRETTY = "text";

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
                            .withValueAttribute(convertPropertyValue(value))
                            .withType(convertPropertyType(value.getClass()))
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
        int size = serverMessage.getEndOfBodyPosition() - serverMessage.getBodyBuffer().readerIndex();
        byte[] buffer = new byte[size];
        serverMessage.getBodyBuffer().readBytes(buffer);
        String value = encode(buffer);

        //requires CDATA
        return BodyType.builder()
            .withValue("<![CDATA[" + value + "]]>")
            .build();
    }

    private String convertPropertyValue(Object value) {
        if (value instanceof byte[]) {
            return encode((byte[]) value).toString();
        }
        return value.toString();
    }

    private MessageType convertAttributes(final ServerMessage message) {
        MessageType messageType = MessageType.builder()
                .withId(message.getMessageID())
                .withTimestamp(message.getTimestamp())
                .withPriority(message.getPriority()).build();

        byte rawType = message.getType();
        String prettyType = DEFAULT_TYPE_PRETTY;
        if (rawType == org.apache.activemq.artemis.api.core.Message.BYTES_TYPE) {
           prettyType = BYTES_TYPE_PRETTY;
        } else if (rawType == org.apache.activemq.artemis.api.core.Message.MAP_TYPE) {
           prettyType = MAP_TYPE_PRETTY;
        } else if (rawType == org.apache.activemq.artemis.api.core.Message.OBJECT_TYPE) {
           prettyType = OBJECT_TYPE_PRETTY;
        } else if (rawType == org.apache.activemq.artemis.api.core.Message.STREAM_TYPE) {
           prettyType = STREAM_TYPE_PRETTY;
        } else if (rawType == org.apache.activemq.artemis.api.core.Message.TEXT_TYPE) {
           prettyType = TEXT_TYPE_PRETTY;
        }

        messageType.setType(prettyType);
        return messageType;
    }

    private String convertPropertyType(Class<?> clazz) {
        if (clazz.equals(SimpleString.class)) {
            return String.class.getSimpleName().toLowerCase();
        }
        return clazz.getSimpleName().toLowerCase();
    }

    private static String encode(final byte[] data) {
        return Base64.encodeBytes(data, 0, data.length, Base64.DONT_BREAK_LINES | Base64.URL_SAFE);
     }

}
