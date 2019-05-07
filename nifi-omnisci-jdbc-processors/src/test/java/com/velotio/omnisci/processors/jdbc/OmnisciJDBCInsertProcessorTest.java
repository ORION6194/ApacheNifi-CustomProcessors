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
import com.google.gson.JsonParser;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import java.util.LinkedList;
import java.util.List;

import static com.velotio.omnisci.utils.db.ProcessorUtils.InsertIntoOmnisciTableJDBC;
import static com.velotio.omnisci.utils.db.ProcessorUtils.ReadOmniSciTableJDBC;
import static org.junit.Assert.*;

public class OmnisciJDBCInsertProcessorTest {

    private TestRunner testRunner;

    @Before
    public void init() {
        testRunner = TestRunners.newTestRunner(OmnisciJDBCJSONInsertProcessor.class);
    }

    @Test
    public void testJSONArrayInsertProcessor() {
        JsonArray jsonBeforeArr = ReadOmniSciTableJDBC("testFlights","*","",0, 0);
        String jsonInput = "[{'arr_timestamp':'2017-04-23 06:30:0','dep_timestamp':'2017-04-23 07:45:00','uniquecarrier':'Southwest'},{'arr_timestamp':'2017-04-23 06:50:0','dep_timestamp':'2017-04-23 09:45:00','uniquecarrier':'American'},{'arr_timestamp':'2017-04-23 09:30:0','dep_timestamp':'2017-04-23 12:45:00','uniquecarrier':'United'}]";
        InsertIntoOmnisciTableJDBC(100,"testFlights","FALSE","",new JsonParser().parse(jsonInput).getAsJsonArray(),"mapd","HyperInteractive","jdbc:omnisci:localhost:6274:mapd");
        JsonArray jsonAfterArr = ReadOmniSciTableJDBC("testFlights","*","",0, 0);
        assertEquals(jsonBeforeArr.size()+3,jsonAfterArr.size());
    }
    @Test
    public void testJSONObjectInsertProcessor() {
        JsonArray jsonBeforeArr = ReadOmniSciTableJDBC("testFlights","*","",0, 0);
        String jsonInput = "{'arr_timestamp':'2017-04-23 06:30:0','dep_timestamp':'2017-04-23 07:45:00','uniquecarrier':'Southwest'}";
        JsonArray jsonInputArr = new JsonArray();
        jsonInputArr.add(new JsonParser().parse(jsonInput).getAsJsonObject());
        InsertIntoOmnisciTableJDBC(100,"testFlights","FALSE","",jsonInputArr,"mapd","HyperInteractive","jdbc:omnisci:localhost:6274:mapd");
        JsonArray jsonAfterArr = ReadOmniSciTableJDBC("testFlights","*","",0, 0);
        assertEquals(jsonBeforeArr.size()+1,jsonAfterArr.size());
    }

    @Test
    public void testInsertProcessorJSON() {
        JsonArray jsonBeforeArr = ReadOmniSciTableJDBC("testFlights","*","",0, 0);
        List<String> jsonInput = new LinkedList<>();
        jsonInput.add("{'arr_timestamp':'2017-04-23 06:30:0','dep_timestamp':'2017-04-23 07:45:00','uniquecarrier':'Southwest'}");
        jsonInput.add("{'arr_timestamp':'2017-04-23 06:50:0','dep_timestamp':'2017-04-23 09:45:00','uniquecarrier':'American'}");
        jsonInput.add("{'arr_timestamp':'2017-04-23 09:30:0','dep_timestamp':'2017-04-23 12:45:00','uniquecarrier':'United'}");
        InsertIntoOmnisciTableJDBC(100,"testFlights","FALSE","",jsonInput,"","mapd","HyperInteractive","jdbc:omnisci:localhost:6274:mapd","JSON");
        JsonArray jsonAfterArr = ReadOmniSciTableJDBC("testFlights","*","",0, 0);
        assertEquals(jsonBeforeArr.size()+3,jsonAfterArr.size());
    }

    @Test
    public void testInsertProcessorCSV() {
        JsonArray jsonBeforeArr = ReadOmniSciTableJDBC("testFlights","*","",0, 0);
        List<String> jsonInput = new LinkedList<>();
        jsonInput.add("2017-04-23 06:30:0, 2017-04-23 07:45:00, Southwest");
        jsonInput.add("2017-04-23 06:50:0, 2017-04-23 09:45:00, American");
        jsonInput.add("2017-04-23 09:30:0, 2017-04-23 12:45:00, United");
        InsertIntoOmnisciTableJDBC(100,"testFlights","FALSE","",jsonInput,", ","mapd","HyperInteractive","jdbc:omnisci:localhost:6274:mapd","Delimited");
        JsonArray jsonAfterArr = ReadOmniSciTableJDBC("testFlights","*","",0, 0);
        assertEquals(jsonBeforeArr.size()+3,jsonAfterArr.size());
    }

}
