/*
 *
 *  Copyright 2013 Netflix, Inc.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */
package com.netflix.zeno.examples;

import com.netflix.zeno.diff.DiffInstruction;
import com.netflix.zeno.diff.DiffOperation;
import com.netflix.zeno.diff.DiffReport;
import com.netflix.zeno.diff.TypeDiff;
import com.netflix.zeno.diff.TypeDiff.FieldDiff;
import com.netflix.zeno.diff.TypeDiff.ObjectDiffScore;
import com.netflix.zeno.diff.TypeDiffInstruction;
import com.netflix.zeno.examples.pojos.A;
import com.netflix.zeno.examples.pojos.B;
import com.netflix.zeno.examples.pojos.C;
import com.netflix.zeno.examples.serializers.ExampleSerializerFactory;
import com.netflix.zeno.fastblob.FastBlobStateEngine;
import com.netflix.zeno.fastblob.io.FastBlobReader;
import com.netflix.zeno.fastblob.io.FastBlobWriter;
import com.netflix.zeno.genericobject.DiffHtmlGenerator;

import java.io.ByteArrayInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.junit.Test;

/**
 * Example usage of the Zeno diff operation.  This operation details the differences between two data states.<p/>
 * 
 * Follow along in the documentation on the page <a href="https://github.com/Netflix/zeno/wiki/Diffing-arbitrary-data-states">diffing arbitrary data states</a>
 * 
 * @author dkoszewnik
 *
 */
public class DiffExample {

    @Test
    public void performDiff() throws IOException {

        /// load two state engines with data states
        FastBlobStateEngine fromStateEngine = getStateEngine();
        FastBlobStateEngine toStateEngine = getAnotherStateEngine();

        /// get a diff "instruction".  This describes how to match up objects
        /// between two FastBlobStateEngines.
        DiffInstruction instruction = getDiffInstruction();
        
        DiffOperation diffOperation = new DiffOperation(new ExampleSerializerFactory(), instruction);

        /// perform the diff report
        DiffReport diffReport = diffOperation.performDiff(fromStateEngine, toStateEngine);

        /// this score can be used as a quick overview to see the magnitude of the differences between the data states
        System.out.println("Total Differences Between Matched Objects: " + diffReport.getTotalDiffs());
        /// this score can be used as a quick overview to see the number of unmatched objects between the data states
        System.out.println("Total Unmatched Objects: " + diffReport.getTotalExtra());

        /// get the differences for a single type
        TypeDiff<A> typeDiff = diffReport.getTypeDiff("A");

        /// iterate through all fields for that type
        for(FieldDiff<A> fieldDiff : typeDiff.getSortedFieldDifferencesDescending()) {
            String propertyName = fieldDiff.getPropertyPath().toString();
            int totalExamples = fieldDiff.getDiffScore().getTotalCount();
            int unmatchedExamples = fieldDiff.getDiffScore().getDiffCount();
            System.out.println(propertyName + ": " + unmatchedExamples + " / " + totalExamples + " were unmatched");
        }

        /// iterate over each of the different instances
        for(ObjectDiffScore<A> objectDiff : typeDiff.getDiffObjects()) {
            A differentA = objectDiff.getFrom();
            System.out.println("A with id " + differentA.getIntValue() + " was different");

            /// show the differences
            displayObjectDifferences(objectDiff.getFrom(), objectDiff.getTo());
        }

    }

    private void displayObjectDifferences(A from, A to) {
        DiffHtmlGenerator generator = new DiffHtmlGenerator(new ExampleSerializerFactory());

        String html = generator.generateDiff("A", from, to);

        displayTheHtml(html);
    }

    private void displayTheHtml(String html) {
        /// for lack of a better thing to do with this.
        /// System.out.println(html);
    }

    public DiffInstruction getDiffInstruction() {
        return new DiffInstruction(
                    new TypeDiffInstruction<A>() {
                        public String getSerializerName() {
                            return "A";
                        }

                        public Object getKey(A object) {
                            return Integer.valueOf(object.getIntValue());
                        }

                    }
                );
    }


    private FastBlobStateEngine getStateEngine() throws IOException {
        return getDeserializedStateEngineWithObjects(
                    getExampleA1(),
                    getExampleA2()
                );
    }

    private FastBlobStateEngine getAnotherStateEngine() throws IOException {
        return getDeserializedStateEngineWithObjects(
                    getExampleA1(),
                    getExampleA2Prime()
                );
    }


    /*
     * Round trip the objects in a FastBlobStateEngine, so they appear in the type deserialization state.
     */
    private FastBlobStateEngine getDeserializedStateEngineWithObjects(A... objects) throws IOException {
        FastBlobStateEngine stateEngine = new FastBlobStateEngine(new ExampleSerializerFactory());

        for(A object : objects) {
            stateEngine.add("A", object);
        }

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dataOutputStream = new DataOutputStream(baos);

        stateEngine.prepareForWrite();

        FastBlobWriter writer = new FastBlobWriter(stateEngine);
        writer.writeSnapshot(dataOutputStream);

        FastBlobReader reader = new FastBlobReader(stateEngine);
        reader.readSnapshot(new ByteArrayInputStream(baos.toByteArray()));

        return stateEngine;
    }


    public A getExampleA1() {
        B b1 = new B(1, "one");
        B b2 = new B(2, "two");

        List<B> bList = new ArrayList<B>();
        bList.add(b1);
        bList.add(b2);

        C c = new C(Long.MAX_VALUE, new byte[] { 1, 2, 3, 4, 5 });

        A a = new A(bList, c, 1);

        return a;
    }

    public A getExampleA2() {
        B b3 = new B(3, "three");
        B b4 = new B(4, "four");
        B b5 = new B(5, "five");

        List<B> bList = new ArrayList<B>();
        bList.add(b3);
        bList.add(b4);
        bList.add(b5);

        C c = new C(Long.MAX_VALUE, new byte[] { 10, 9, 8, 7, 6 });

        A a = new A(bList, c, 2);

        return a;
    }

    public A getExampleA2Prime() {
        B c3 = new B(3, "three");
        B c4 = new B(4, "four");
        B c6 = new B(6, "six");

        List<B> cList = new ArrayList<B>();
        cList.add(c3);
        cList.add(c4);
        cList.add(c6);

        C d = new C(Long.MAX_VALUE, new byte[] { 10, 9, 8, 7, 6 });

        A a = new A(cList, d, 2);

        return a;
    }


}
