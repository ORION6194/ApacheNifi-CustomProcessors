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

import com.google.gson.*;
import com.velotio.omnisci.utils.db.ProcessorUtils;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.stream.io.StreamUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Tags({"Omnisci","JDBC","JSON","INSERT"})
@CapabilityDescription("This Processor takes a JSON Array as an Input and Inserts the data into OmnisciDB in batches using JDBC Prepared statements")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
public class OmnisciJDBCJSONInsertProcessor extends AbstractProcessor {

    public static final PropertyDescriptor USER_NAME = new PropertyDescriptor
            .Builder().name("USER_NAME")
            .displayName("UserName")
            .description("UserName for the Omnisci DB")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor PASSWORD = new PropertyDescriptor
            .Builder().name("PASSWORD")
            .displayName("Password")
            .description("Password for the Omnisci DB")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor DB_URL = new PropertyDescriptor
            .Builder().name("DB_URL")
            .displayName("DB Host URL Details")
            .description("Hostname and Port No for the Omnisci DB. Ex: 'jdbc:omnisci:<hostname>:<port>:<database name>'")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor TABLE_NAME = new PropertyDescriptor
            .Builder().name("TABLE_NAME")
            .displayName("Table Name")
            .description("Name of Table in which to insert the data")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor CREATE_NEW_TABLE = new PropertyDescriptor
            .Builder().name("CREATE_NEW_TABLE")
            .displayName("Create New Table")
            .description("A boolean value to determine if a new table needs to be created or not")
            .allowableValues("TRUE","FALSE")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor TABLE_SCHEMA = new PropertyDescriptor
            .Builder().name("TABLE_SCHEMA")
            .displayName("Table Schema")
            .description("Schema of the table in which to insert the data")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor OMNISCI_INSERT_BATCH_SIZE = new PropertyDescriptor
            .Builder().name("OMNISCI_INSERT_BATCH_SIZE")
            .displayName("Omnisci Insert Batch Size")
            .description("Batch size for the Omnisci JDBC Prepared Statement")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();


    public static final Relationship SUCCESS = new Relationship.Builder()
            .name("SUCCESS")
            .description("Success Relationship")
            .build();

    public static final Relationship FAILURE = new Relationship.Builder()
            .name("FAILURE")
            .description("Failure Relationship")
            .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(DB_URL);
        descriptors.add(USER_NAME);
        descriptors.add(PASSWORD);
        descriptors.add(TABLE_NAME);
        descriptors.add(TABLE_SCHEMA);
        descriptors.add(OMNISCI_INSERT_BATCH_SIZE);
        descriptors.add(CREATE_NEW_TABLE);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(SUCCESS);
        relationships.add(FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {

    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if ( flowFile == null ) {
            return;
        }
        try {
            final String createNewTable = context.getProperty("CREATE_NEW_TABLE").getValue();
            final String tableName = context.getProperty("TABLE_NAME").getValue();
            final String tableSchema = context.getProperty("TABLE_SCHEMA").getValue();
            final String userName = context.getProperty("USER_NAME").getValue();
            final String password = context.getProperty("PASSWORD").getValue();
            final String dbURL = context.getProperty("DB_URL").getValue();
            final int insertBatchSize = Integer.parseInt(context.getProperty("OMNISCI_INSERT_BATCH_SIZE").getValue());

            if(createNewTable.equals("TRUE") && (tableSchema==null || tableSchema.equals(""))){
                throw new Exception("'Table Schema' value is Required when 'Create New Table' field is set to 'TRUE'");
            }

            // reading all the content of the input flow file
            final byte[] byteBuffer = new byte[(int) flowFile.getSize()];
            session.read(flowFile, new InputStreamCallback() {
                @Override
                public void process(InputStream in) throws IOException {
                    StreamUtils.fillBuffer(in, byteBuffer, false);
                }
            });

            // convert the content into a JSON object
            final String contentString = new String(byteBuffer, 0, byteBuffer.length, Charset.forName("UTF-8"));
            try {
                JsonArray jsonArr = new JsonParser().parse(contentString).getAsJsonArray();
                //Insert into the OmnisciDB
                String returnResponse = ProcessorUtils.InsertIntoOmnisciTableJDBC(insertBatchSize, tableName, createNewTable, tableSchema, jsonArr, userName, password, dbURL);
                flowFile = session.putAttribute(flowFile, "JDBCArrOutput",returnResponse);
            }catch (IllegalStateException ise){
                JsonArray jsonArr = new JsonArray();
                JsonObject jsonObj = new JsonParser().parse(contentString).getAsJsonObject();
                jsonArr.add(jsonObj);
                String returnResponse = ProcessorUtils.InsertIntoOmnisciTableJDBC(insertBatchSize, tableName, createNewTable, tableSchema, jsonArr, userName, password, dbURL);
                flowFile = session.putAttribute(flowFile, "JDBCObjOutput",returnResponse);
            }
            session.getProvenanceReporter().modifyContent(flowFile);
            session.transfer(flowFile, SUCCESS);
        }catch (Exception e){
            String errorMsg = e.getMessage();
            // write the processed data in the content of the output flow file
            flowFile = session.write(flowFile, new OutputStreamCallback() {
                @Override
                public void process(final OutputStream out) throws IOException {
                    out.write(errorMsg.getBytes());
                }
            });
            session.transfer(flowFile, FAILURE);
        }
    }
}