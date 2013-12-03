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

import com.netflix.zeno.examples.pojos.A;
import com.netflix.zeno.examples.pojos.B;
import com.netflix.zeno.examples.pojos.C;
import com.netflix.zeno.examples.serializers.ExampleSerializerFactory;
import com.netflix.zeno.fastblob.FastBlobStateEngine;
import com.netflix.zeno.fastblob.io.FastBlobReader;
import com.netflix.zeno.fastblob.io.FastBlobWriter;
import com.netflix.zeno.fastblob.state.FastBlobTypeDeserializationState;

import java.io.ByteArrayInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.junit.Test;

/**
 * This is similar to the example shown in the {@link BasicSerializationExample}, except
 * that multiple images are now produced.  Follow along in the Zeno documentation on
 * the page <a href="https://github.com/Netflix/zeno/wiki/Producing-multiple-images>producing multiple images</a>
 * 
 * @author dkoszewnik
 *
 */
public class MultipleImageSerializationExample {

    private FastBlobStateEngine stateEngine;

    private byte[] image1Snapshot;
    private byte[] image2Snapshot;

    private byte[] image1Delta;
    private byte[] image2Delta;

    @Test
    public void basicSerializationCycle() {

        /// First we create a state engine, we need to tell it about our data model by
        /// passing it a serializer factory which creates our top-level serializers.
        /// Here, we're also telling it that we will be creating two separate images.
        stateEngine = new FastBlobStateEngine(new ExampleSerializerFactory(), 2);

        /// For this example, we're just storing the serialized data in memory.
        createSnapshot();
        createDelta();

        /// Now we can pretend to be the client, and deserialize the data into a separate state engine.
        /// TODO: Try changing these to be image1Snapshot and image1Delta to observe the results.
        FastBlobStateEngine clientStateEngine = deserializeLatestData(image1Snapshot, image1Delta);

        System.out.println("StateEngine 1:");
        showValuesForStateEngine(clientStateEngine);

        clientStateEngine = deserializeLatestData(image2Snapshot, image2Delta);

        System.out.println("StateEngine 2:");
        showValuesForStateEngine(clientStateEngine);
    }

    private void showValuesForStateEngine(FastBlobStateEngine clientStateEngine) {
        /// We can grab the data for any class.  Here, we grab the values for class A.
        FastBlobTypeDeserializationState<A> aState = clientStateEngine.getTypeDeserializationState("A");

        /// the following will loop through all values after loading the snapshot
        /// and subsequently applying the delta.
        System.out.println("All As: ");
        for(A deserializedA : aState){
            System.out.println(deserializedA.getIntValue());
        }

        /// As another example, we can grab the values for class B.
        FastBlobTypeDeserializationState<B> bState = clientStateEngine.getTypeDeserializationState("B");

        /// Even though we didn't directly add the Bs, they were added because we added objects which
        /// referenced them.  Here, we iterate over all of the Bs after applying the delta.
        System.out.println("All Bs: ");
        for(B deserializedB : bState) {
            System.out.println(deserializedB.getBInt());
        }
    }

    public FastBlobStateEngine deserializeLatestData(byte snapshot[], byte delta[]) {
        /// now we are on the client.  We need to create a state engine, and again
        /// tell it about our data model.
        FastBlobStateEngine stateEngine = new FastBlobStateEngine(new ExampleSerializerFactory());

        /// we need to create a FastBlobReader, which is responsible for reading
        /// serialized blobs.
        FastBlobReader reader = new FastBlobReader(stateEngine);

        /// get a stream from the snapshot file location
        ByteArrayInputStream snapshotStream = new ByteArrayInputStream(snapshot);
        /// get a stream from the delta file location
        ByteArrayInputStream deltaStream = new ByteArrayInputStream(delta);


        try {
            /// first read the snapshot
            reader.readSnapshot(snapshotStream);
            /// then apply the delta
            reader.readDelta(deltaStream);
        } catch (IOException e) {
            /// either of these methods throws an exception if the FastBlobReader
            /// is unable to read from the provided stream.
        } finally {
            /// it is your responsibility to close the streams.  The FastBlobReader will not do this.
            try {
                snapshotStream.close();
                deltaStream.close();
            } catch (IOException ignore) { }
        }

        return stateEngine;
    }

    public void createSnapshot() {
        /// We add each of our object instances to the state engine.  Note that for each call, we are
        /// specifying the images to which we are adding the instance.
        /// This operation is still thread safe and can be called from multiple processing threads.
        stateEngine.add("A", getExampleA1(), new boolean[] { true, false });
        stateEngine.add("A", getExampleA2(), new boolean[] { true, true });
        stateEngine.add("A", getExampleA3(), new boolean[] { false, true });

        /// The following lets the state engine know that we have finished adding all of our
        /// objects to the state.
        stateEngine.prepareForWrite();

        /// Create a writer, which will be responsible for creating snapshot and/or delta blobs.
        /// Note that we specify the index of the image we wish to write.
        FastBlobWriter writer = new FastBlobWriter(stateEngine, 0);

        image1Snapshot = writeSnapshot(writer);
        image2Snapshot = writeSnapshot(new FastBlobWriter(stateEngine, 1));
    }

    private byte[] writeSnapshot(FastBlobWriter writer) {
        /// Create an output stream to somewhere.  This can be to a local file on disk
        /// or directly to the blob destination.  The VMS team writes this data directly
        /// to disk, and then spawns a new thread to upload the data to S3.
        /// We need to pass a DataOutputStream.  Remember that DataOutputStream is not buffered,
        /// so if you write to a FileOutputStream or other non-buffered source, you likely want to
        /// make the DataOutputStream wrap a BufferedOutputStream
        /// (e.g. new DataOutputStream(new BufferedOutputStream(new FileOutputStream(destinationFile))))
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream outputStream = new DataOutputStream(baos);

        try {
            /// write the snapshot to the output stream.
            writer.writeSnapshot(outputStream);
        } catch(IOException e) {
            /// the FastBlobWriter throws an IOException if it is
            /// unable to write to the provided OutputStream
        } finally {
            /// it is your responsibility to close the stream.  The FastBlobWriter will not do this.
            try {
                outputStream.close();
            } catch(IOException ignore) { }
        }

        byte snapshot[] = baos.toByteArray();

        return snapshot;
    }

    public void createDelta() {
        /// The following call informs the state engine that we have finished writing
        /// all of our snapshot / delta files, and we are ready to begin adding fresh
        /// object instances for the next cycle.
        stateEngine.prepareForNextCycle();

        // Again, we add each of our object instances to the state engine.
        // This operation is still thread safe and can be called from multiple processing threads.
        stateEngine.add("A", getExampleA1(), new boolean[] { true, false });
        stateEngine.add("A", getExampleA2Prime(), new boolean[] { true, true });
        stateEngine.add("A", getExampleA3(), new boolean[] { false, true });

        /// We must again let the state engine know that we have finished adding all of our
        /// objects to the state.
        stateEngine.prepareForWrite();

        /// Create a writer, which will be responsible for creating snapshot and/or delta blobs.
        /// Again, note that we are specifying the index of image we wish to write.
        FastBlobWriter writer = new FastBlobWriter(stateEngine, 0);

        image1Delta = writeDelta(writer);
        image2Delta = writeDelta(new FastBlobWriter(stateEngine, 1));
    }

    private byte[] writeDelta(FastBlobWriter writer) {
        /// Again create an output stream
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream outputStream = new DataOutputStream(baos);

        try {
            /// This time write the delta file.
            writer.writeDelta(outputStream);
        } catch(IOException e) {
            /// thrown if the FastBlobWriter was unable to write to the provided stream.
        } finally {
            try {
                outputStream.close();
            } catch(IOException ignore){ }
        }

        byte delta[] = baos.toByteArray();

        return delta;
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

    public A getExampleA3() {
        B b3 = new B(8, "eight");

        List<B> bList = new ArrayList<B>();
        bList.add(b3);

        C c = new C(Long.MAX_VALUE, new byte[] { 20, 30, 40 });

        A a = new A(bList, c, 3);

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
