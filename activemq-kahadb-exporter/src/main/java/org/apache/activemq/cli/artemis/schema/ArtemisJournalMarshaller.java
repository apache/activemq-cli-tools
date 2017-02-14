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

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.regex.Pattern;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.namespace.QName;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

import org.apache.activemq.artemis.cli.commands.tools.XmlDataConstants;
import org.apache.activemq.cli.schema.ActivemqJournalType;
import org.apache.activemq.cli.schema.AddressBindingType;
import org.apache.activemq.cli.schema.MessageType;
import org.apache.activemq.cli.schema.ObjectFactory;
import org.apache.activemq.cli.schema.QueueBindingType;

/**
 * Marshaller class to stream to a file
 */
public class ArtemisJournalMarshaller {

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

        PrettyPrintHandler handler = new PrettyPrintHandler(xmlWriter);
        this.xmlWriter = (XMLStreamWriter) Proxy.newProxyInstance(XMLStreamWriter.class.getClassLoader(), new Class[]{XMLStreamWriter.class}, handler);
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
        xmlWriter.writeStartElement(XmlDataConstants.MESSAGES_PARENT);
    }

    public void appendBindingsElement() throws XMLStreamException {
        xmlWriter.writeStartElement(XmlDataConstants.BINDINGS_PARENT);
    }

    public void appendEndElement() throws XMLStreamException {
        xmlWriter.writeEndElement();
    }

    public void appendMessage(final MessageType message) throws JAXBException {
        marshaller.marshal(wrap(XmlDataConstants.MESSAGES_CHILD, message), xmlWriter);
    }

    public void appendBinding(final AddressBindingType addressBinding) throws JAXBException {
        marshaller.marshal(wrap(XmlDataConstants.ADDRESS_BINDINGS_CHILD, addressBinding), xmlWriter);
    }

    public void appendBinding(final QueueBindingType queueBinding) throws JAXBException {
        marshaller.marshal(wrap(XmlDataConstants.QUEUE_BINDINGS_CHILD, queueBinding), xmlWriter);
    }

    @SuppressWarnings("unchecked")
    private <T> JAXBElement<T> wrap(String name, T object) {
        return new JAXBElement<T>(QName.valueOf(name), (Class<T>) object.getClass(), object);
    }

    static class PrettyPrintHandler implements InvocationHandler {

        private static final Pattern XML_CHARS = Pattern.compile( "[&<>]" );

        private final XMLStreamWriter target;

        private int depth = 0;

        private static final char INDENT_CHAR = ' ';

        private static final String LINE_SEPARATOR = System.getProperty("line.separator");

        boolean wrap = true;

        PrettyPrintHandler(XMLStreamWriter target) {
           this.target = target;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
           String m = method.getName();
           boolean useCData = false;

           switch (m) {
              case "writeStartElement":
                 target.writeCharacters(LINE_SEPARATOR);
                 target.writeCharacters(indent(depth));

                 depth++;
                 break;
              case "writeEndElement":
                 depth--;
                 if (wrap) {
                    target.writeCharacters(LINE_SEPARATOR);
                    target.writeCharacters(indent(depth));
                 }
                 wrap = true;
                 break;
              case "writeEmptyElement":
              case "writeCData":
                 target.writeCharacters(LINE_SEPARATOR);
                 target.writeCharacters(indent(depth));
                 break;
              case "writeCharacters":
                 useCData = XML_CHARS.matcher( (String)args[0] ).find();
                 if (!useCData) {
                     wrap = false;
                     break;
                 } else {
                     target.writeCharacters(LINE_SEPARATOR);
                     target.writeCharacters(indent(depth));
                     break;
                 }
           }

           if (useCData) {
               Method cdata = XMLStreamWriter.class.getMethod("writeCData", String.class);
               args[0] = ((String)args[0]).replace("<![CDATA[", "").replace("]]>", "");
               cdata.invoke(target, args);
           } else {
               method.invoke(target, args);
           }

           return null;
        }

        private String indent(int depth) {
           depth *= 3; // level of indentation
           char[] output = new char[depth];
           while (depth-- > 0) {
              output[depth] = INDENT_CHAR;
           }
           return new String(output);
        }
     }
}
