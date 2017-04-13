/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.falcon.regression.Extensions;

import org.apache.falcon.Pair;
import org.apache.falcon.entity.v0.Entity;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.feed.Schema;
import org.apache.falcon.entity.v0.process.Process;
import org.apache.falcon.extensions.ExtensionBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.JAXBException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;


class ExtensionExampleUtil implements ExtensionBuilder{

    private static final Logger LOG = LoggerFactory.getLogger(ExtensionExampleUtil.class);
    private static final String PROCESS_XML = "/extension-example.xml";

    @Override
    public List<Entity> getEntities(String extensionName, InputStream extensionConfigStream)
        throws org.apache.falcon.FalconException {
        Process process;
        try {
            process = (Process) EntityType.PROCESS.getUnmarshaller().unmarshal(
                    getClass().getResourceAsStream(PROCESS_XML));
        } catch (JAXBException e) {
            throw new org.apache.falcon.FalconException("Failed in un-marshalling the entity");
        }
        if (extensionConfigStream != null) {
            Properties properties = new Properties();
            try {
                properties.load(extensionConfigStream);
            } catch (IOException e) {
                LOG.warn("Not able to load the configStream");
            }
            process.setPipelines(properties.getProperty("pipelines.name"));
        }
        List<Entity> entities = new ArrayList<>();
        entities.add(process);
        return entities;
    }

    @Override
    public void validateExtensionConfig(String extensionName, InputStream extensionConfigStream)
        throws org.apache.falcon.FalconException {

    }

    @Override
    public List<Pair<String, Schema>> getOutputSchemas(String extensionName)
        throws org.apache.falcon.FalconException {
        return null;
    }

    public String toString(String testString) {
        return testString;
    }

}
