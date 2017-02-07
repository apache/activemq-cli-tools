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
package org.apache.activemq.cli.artemis.schema;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.namespace.QName;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

import org.apache.activemq.cli.schema.ActivemqJournalType;
import org.apache.activemq.cli.schema.AddressBindingType;
import org.apache.activemq.cli.schema.MessageType;
import org.apache.activemq.cli.schema.ObjectFactory;
import org.apache.activemq.cli.schema.QueueBindingType;

/**
 * Marshaller class to stream to a file
 */
public class ArtemisJournalMarshaller {

    public final static String MESSAGE_ELEMENT = "message";
    public final static String MESSAGES_ELEMENT = "messages";
    public final static String BINDINGS_ELEMENT = "bindings";
    public final static String ADDRESS_BINDING_ELEMENT = "address-binding";
    public final static String QUEUE_BINDING_ELEMENT = "queue-binding";

    private final ObjectFactory factory = new ObjectFactory();
    private final JAXBContext context;
    private final Marshaller marshaller;
    private final XMLStreamWriter xmlWriter;
    //Used for journal start
    final JAXBElement<ActivemqJournalType> journalElement =
            factory.createActivemqJournal(new ActivemqJournalType());

    public ArtemisJournalMarshaller(final XMLStreamWriter xmlWriter) throws JAXBException {
        this.context = JAXBContext.newInstance(ObjectFactory.class);
        this.marshaller = context.createMarshaller();
        this.marshaller.setProperty(Marshaller.JAXB_FRAGMENT, Boolean.TRUE);
        this.xmlWriter = xmlWriter;
    }

    public void appendJournalOpen() throws XMLStreamException {
        xmlWriter.writeStartDocument();
        xmlWriter.writeStartElement(journalElement.getName().toString());
    }

    public void appendJournalClose(boolean closeWriter) throws XMLStreamException {
        xmlWriter.writeEndDocument();
        if (closeWriter) {
            xmlWriter.flush();
            xmlWriter.close();
        }
    }

    public void appendMessagesElement() throws XMLStreamException {
        xmlWriter.writeStartElement(MESSAGES_ELEMENT);
    }

    public void appendBindingsElement() throws XMLStreamException {
        xmlWriter.writeStartElement(BINDINGS_ELEMENT);
    }

    public void appendEndElement() throws XMLStreamException {
        xmlWriter.writeEndElement();
    }

    public void appendMessage(final MessageType message) throws JAXBException {
        marshaller.marshal(wrap(MESSAGE_ELEMENT, message), xmlWriter);
    }

    public void appendBinding(final AddressBindingType addressBinding) throws JAXBException {
        marshaller.marshal(wrap(ADDRESS_BINDING_ELEMENT, addressBinding), xmlWriter);
    }

    public void appendBinding(final QueueBindingType queueBinding) throws JAXBException {
        marshaller.marshal(wrap(QUEUE_BINDING_ELEMENT, queueBinding), xmlWriter);
    }

    @SuppressWarnings("unchecked")
    private <T> JAXBElement<T> wrap(String name, T object) {
        return new JAXBElement<T>(QName.valueOf(name), (Class<T>) object.getClass(), object);
    }
}
