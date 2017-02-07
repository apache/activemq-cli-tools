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

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.FileOutputStream;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamWriter;

import org.apache.activemq.cli.artemis.schema.ArtemisJournalMarshaller;
import org.apache.activemq.cli.schema.ActivemqJournalType;
import org.apache.activemq.cli.schema.AddressBindingType;
import org.apache.activemq.cli.schema.MessageType;
import org.apache.activemq.cli.schema.MessagesType;
import org.apache.activemq.cli.schema.ObjectFactory;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class ArtemisJournalMarshallerTest {

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    /**
     * Test default marshaling of entire document at once
     *
     * @throws Exception
     */
    @Test
    public void testFullMarshal() throws Exception {

        ObjectFactory factory = new ObjectFactory();

        ActivemqJournalType journal = new ActivemqJournalType();
        MessagesType messages = factory.createMessagesType();
        for (int i = 0; i < 3; i++) {
            MessageType message = factory.createMessageType();
            message.setId((long) i);
            messages.getMessage().add(message);
        }
        journal.setMessages(messages);

        JAXBContext context = JAXBContext.newInstance(ActivemqJournalType.class);
        Marshaller m = context.createMarshaller();
        m.setProperty(Marshaller.JAXB_FRAGMENT, Boolean.TRUE);

        File file = tempFolder.newFile();
        JAXBElement<ActivemqJournalType> element = factory.createActivemqJournal(journal);
        m.marshal(element, file);
        validate(file);
    }

    /**
     * Test stream marshal by appending one message at a time
     *
     * @throws Exception
     */
    @Test
    public void testStreamMarshal() throws Exception {
        File file = tempFolder.newFile();

        try(FileOutputStream fos = new FileOutputStream(file)) {
            XMLStreamWriter xmlWriter = XMLOutputFactory.newFactory().createXMLStreamWriter(fos);
            ArtemisJournalMarshaller xmlMarshaller = new ArtemisJournalMarshaller(xmlWriter);

            xmlMarshaller.appendJournalOpen();
            xmlMarshaller.appendBindingsElement();
            xmlMarshaller.appendBinding(new AddressBindingType());
            xmlMarshaller.appendEndElement();
            xmlMarshaller.appendMessagesElement();

            //Marshal messages one at a time
            for (int i = 0; i < 3; i++) {
                MessageType message = new MessageType();
                message.setId((long) i);
                message.setTimestamp(System.currentTimeMillis());
                xmlMarshaller.appendMessage(message);
            }

            xmlMarshaller.appendEndElement();
            xmlMarshaller.appendJournalClose(true);
        }

        //This can be read as a stream as well but for the purpose of this test
        //just read the whole thing in at once
        validate(file);
    }

    @SuppressWarnings("unchecked")
    private void validate(File file) throws JAXBException {
        JAXBContext jaxbContext = JAXBContext.newInstance(ObjectFactory.class);
        Unmarshaller jaxbUnmarshaller = jaxbContext.createUnmarshaller();
        JAXBElement<ActivemqJournalType> read = (JAXBElement<ActivemqJournalType>) jaxbUnmarshaller.unmarshal(file);
        assertEquals(3, read.getValue().getMessages().getMessage().size());
    }

}
