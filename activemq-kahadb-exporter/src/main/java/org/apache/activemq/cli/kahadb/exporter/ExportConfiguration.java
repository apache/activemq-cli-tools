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
import java.util.HashMap;
import java.util.Map;

import org.apache.activemq.artemis.utils.CompositeAddress;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.filter.DestinationFilter;
import org.apache.activemq.filter.DestinationPath;

public class ExportConfiguration {

    private File source;

    private File target;

    private String queuePattern;

    private String topicPattern;

    private boolean multiKaha;

    private boolean compress;

    private boolean overwrite;

    private final Map<DestinationFilter, Integer> vtConsumerDestinationMatchers = new HashMap<>();

    public File getSource() {
        return source;
    }

    public void setSource(File source) {
        this.source = source;
    }

    public File getTarget() {
        return target;
    }

    public void setTarget(File target) {
        this.target = target;
    }

    public String getQueuePattern() {
        return queuePattern;
    }

    public void setQueuePattern(String queuePattern) {
        this.queuePattern = queuePattern;
    }

    public String getTopicPattern() {
        return topicPattern;
    }

    public void setTopicPattern(String topicPattern) {
        this.topicPattern = topicPattern;
    }

    public boolean isMultiKaha() {
        return multiKaha;
    }

    public void setMultiKaha(boolean multiKaha) {
        this.multiKaha = multiKaha;
    }

    public boolean isCompress() {
        return compress;
    }

    public void setCompress(boolean compress) {
        this.compress = compress;
    }

    public boolean isOverwrite() {
        return overwrite;
    }

    public void setOverwrite(boolean overwrite) {
        this.overwrite = overwrite;
    }

    public void setVirtualTopicConsumerWildcards(String virtualTopicConsumerWildcards) {
        if (virtualTopicConsumerWildcards != null) {
            for (String filter : virtualTopicConsumerWildcards.split(",")) {
                String[] wildcardLimitPair = filter.split(";");
                vtConsumerDestinationMatchers.put(DestinationFilter.parseFilter(new ActiveMQQueue(wildcardLimitPair[0])), Integer.valueOf(wildcardLimitPair[1]));
            }
        }
    }

    public ActiveMQDestination mapToDurableSubFQQN(ActiveMQDestination destination) {

        if (vtConsumerDestinationMatchers.isEmpty()) {
            return destination;
        }

        for (Map.Entry<DestinationFilter, Integer> candidate : vtConsumerDestinationMatchers.entrySet()) {
            if (candidate.getKey().matches(destination)) {
                // convert to matching FQQN
                String[] paths = DestinationPath.getDestinationPaths(destination);
                StringBuilder fqqn = new StringBuilder();
                int filterPathTerminus = candidate.getValue();
                // address - ie: topic
                for (int i = filterPathTerminus; i < paths.length; i++) {
                    if (i > filterPathTerminus) {
                        fqqn.append(ActiveMQDestination.PATH_SEPERATOR);
                    }
                    fqqn.append(paths[i]);
                }
                fqqn.append(CompositeAddress.SEPARATOR);
                // consumer queue - the full vt queue
                for (int i = 0; i < paths.length; i++) {
                    if (i > 0) {
                        fqqn.append(ActiveMQDestination.PATH_SEPERATOR);
                    }
                    fqqn.append(paths[i]);
                }
                // no need for a cache as this is called once per destination on metadata export
                return new ActiveMQQueue(fqqn.toString());
            }
        }
        return destination;
    }

    public static class ExportConfigurationBuilder {

        private final ExportConfiguration config = new ExportConfiguration();

        public static ExportConfigurationBuilder newBuilder() {
            return new ExportConfigurationBuilder();
        }

        public ExportConfigurationBuilder setSource(File source) {
            config.setSource(source);
            return this;
        }

        public ExportConfigurationBuilder setTarget(File target) {
            config.setTarget(target);
            return this;
        }

        public ExportConfigurationBuilder setMultiKaha(boolean multiKaha) {
            config.setMultiKaha(multiKaha);
            return this;
        }

        public ExportConfigurationBuilder setQueuePattern(String queuePattern) {
            config.setQueuePattern(queuePattern);
            return this;
        }

        public ExportConfigurationBuilder setTopicPattern(String topicPattern) {
            config.setTopicPattern(topicPattern);
            return this;
        }

        public ExportConfigurationBuilder setCompress(boolean compress) {
            config.setCompress(compress);
            return this;
        }

        public ExportConfigurationBuilder setOverwrite(boolean overwrite) {
            config.setOverwrite(overwrite);
            return this;
        }

        public ExportConfiguration build() {
            return config;
        }

        public ExportConfigurationBuilder setVirtualTopicConsumerWildcards(String virtualTopicConsumerWildcards) {
            config.setVirtualTopicConsumerWildcards(virtualTopicConsumerWildcards);
            return this;
        }
    }
}
