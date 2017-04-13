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

package org.apache.falcon.regression.userExtensions;

import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.helpers.entity.AbstractEntityHelper;
import org.apache.falcon.regression.core.response.ServiceResponse;
import org.apache.falcon.regression.core.util.AssertUtil;
import org.apache.falcon.regression.core.util.HadoopUtil;
import org.apache.falcon.regression.core.util.OSUtil;
import org.apache.falcon.regression.testHelper.BaseTestClass;
import org.apache.hadoop.fs.FileSystem;
import org.apache.oozie.client.OozieClient;
import org.testng.annotations.*;
import org.apache.log4j.Logger;

import java.util.Arrays;
import java.util.List;

@Test(groups = "distributed")
public class RegisterUnregisterTest extends BaseTestClass {

    private static final Logger LOGGER = Logger.getLogger(RegisterUnregisterTest.class);
    private ColoHelper cluster = servers.get(0);
    private FileSystem clusterFS = serverFS.get(0);
    private OozieClient clusterOC = serverOC.get(0);
    private String baseTestDir = cleanAndGetTestDir();

    @BeforeClass(alwaysRun = true)
    public void uploadExtension() throws Exception{
        String folderPrefix = "/tmp/extensions/extension-example/";
        HadoopUtil.recreateDir(clusterFS, folderPrefix);
        List<String> folderPath = Arrays.asList("META/config","README","libs/build","libs/runtime","resources/build","resources/runtime");
        HadoopUtil.createFolders(clusterFS, folderPrefix, folderPath);
        String destination = folderPrefix+"/libs/build/";
        HadoopUtil.copyDataToFolder(clusterFS, destination, OSUtil.concat(OSUtil.EXTENSIONS,"falcon-merlin-core-0.11-SNAPSHOT.jar"));
    }

    @Test
    public static void registerExtension() {
        AbstractEntityHelper helper = new AbstractEntityHelper("prism") {
            @Override
            public String getEntityType() {
                return null;
            }

            @Override
            public String getEntityName(String entity) {
                return null;
            }
        };
        String params="path=hdfs://192.168.138.236:8020/tmp/extensions/extension-example";
        try {
            final ServiceResponse serviceResponse = helper.extensionRegister(params);
            LOGGER.info("abc");
//            AssertUtil.assertSucceeded(serviceResponse);
        }
        catch (Exception e) {
            e.printStackTrace();
            System.out.println(e.getMessage());

        }

    }

}
