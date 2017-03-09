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

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import io.airlift.airline.ParseOptionMissingException;

@RunWith(Parameterized.class)
public class ExporterCLITest {

    @Rule
    public TemporaryFolder temp = new TemporaryFolder();
    private String type;

    @Parameters(name = "type={0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
                {"kahadb"},
                {"mkahadb"}
        });
    }

    public ExporterCLITest(String type) {
        super();
        this.type = type;
    }

    @Test(expected = ParseOptionMissingException.class)
    public void testMissingsArgs() {
        Exporter.main(new String[]{type});
    }

    @Test(expected = ParseOptionMissingException.class)
    public void testMissingTarget() {
        Exporter.main(new String[]{type, "-s", "file"});
    }

    @Test(expected = ParseOptionMissingException.class)
    public void testMissingSource() {
        Exporter.main(new String[]{type, "-t", "file"});
    }

    @Test(expected = IllegalStateException.class)
    public void testFileExists() throws IOException {
        Exporter.main(new String[]{type, "-s", temp.newFolder().getAbsolutePath(),
                "-t", temp.newFile().getAbsolutePath()});
    }

    @Test
    public void testFileOverwrite() throws IOException {
        Exporter.main(new String[]{type, "-s", temp.newFolder().getAbsolutePath(),
                "-t", temp.newFile().getAbsolutePath(), "-f"});
        //should be no exceptions
    }
}
