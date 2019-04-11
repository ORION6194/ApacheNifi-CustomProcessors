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

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonParser;
import com.velotio.omnisci.utils.ProcessorUtils;
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

@Tags({"Omnisci","JDBC","INSERT"})
@CapabilityDescription("Provide a description")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
public class OmnisciJDBCInsertProcessor extends AbstractProcessor {

    public static final PropertyDescriptor TABLE_NAME = new PropertyDescriptor
            .Builder().name("TABLE_NAME")
            .displayName("Table Name")
            .description("Name of Table in which to insert the data")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor TABLE_SCHEMA = new PropertyDescriptor
            .Builder().name("TABLE_SCHEMA")
            .displayName("Table Schema")
            .description("Schema of the table in which to insert the data")
            .required(true)
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
        descriptors.add(TABLE_NAME);
        descriptors.add(TABLE_SCHEMA);
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
            JsonParser parser = new JsonParser();
            JsonArray jsonArr = parser.parse(contentString).getAsJsonArray();
            flowFile = session.putAttribute(flowFile, "Table_Name", context.getProperty("TABLE_NAME").getValue());
            //Insert into the OmnisciDB

            String returnResponse = ProcessorUtils.InsertIntoOmnisciTableJDBC("newflights",jsonArr);
            flowFile = session.putAttribute(flowFile, "jsonInput",jsonArr.toString());
            flowFile = session.putAttribute(flowFile, "JDBCOutput",returnResponse);
//            ProcessorUtils.InsertIntoOmnisciTableJDBC(context.getProperty("TABLE_NAME").getValue(),jsonArr);

//            flowFile = session.putAttribute(flowFile, "mime.type", "application/json");
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
