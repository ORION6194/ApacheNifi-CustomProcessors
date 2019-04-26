/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.velotio.omnisci.processors.jdbc;

import com.google.gson.JsonArray;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

import static com.velotio.omnisci.utils.db.ProcessorUtils.ReadOmniSciTableJDBC;


public class OmnisciJDBCReadProcessorTest {

    private TestRunner testRunner;

    @Before
    public void init() {
        testRunner = TestRunners.newTestRunner(OmnisciJDBCReadJSONProcessor.class);
    }

    @Test
    public void testProcessor() {
        JsonArray jsonArray = ReadOmniSciTableJDBC("flights_2008_7M","origin_city AS \"Origin\", dest_city AS \"Destination\", AVG(airtime) AS \"Average Airtime\"","WHERE distance < 175 GROUP BY origin_city, dest_city",100,0 );
        assertEquals(100,jsonArray.size());
    }

}
