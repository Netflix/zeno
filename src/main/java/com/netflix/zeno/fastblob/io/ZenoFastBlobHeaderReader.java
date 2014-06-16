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
package com.netflix.zeno.fastblob.io;

import com.netflix.zeno.fastblob.record.VarInt;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

public class ZenoFastBlobHeaderReader implements FastBlobHeaderReader {

    @Override
    public FastBlobHeader readHeader(InputStream is) throws IOException, FastBlobMalformedDataException {
        FastBlobHeader header = new FastBlobHeader();
        DataInputStream dis = new DataInputStream(is);

        int headerVersion = dis.readInt();
        if(headerVersion != FastBlobHeader.FAST_BLOB_VERSION_HEADER) {
            throw new FastBlobMalformedDataException("The FastBlob you are trying to read is incompatible.  The expected FastBlob version was " + FastBlobHeader.FAST_BLOB_VERSION_HEADER + " but the actual version was " + headerVersion);
        }

        String latestVersion = dis.readUTF();
        header.setVersion(latestVersion);

        Map<String, String> headerTags = readHeaderTags(dis);
        header.setHeaderTags(headerTags);

        int deserializationBufferSizeHint = is.read();
        header.setDeserializationBufferSizeHint(deserializationBufferSizeHint);

        dis.read(); //Flags byte. Reserved for later use

        int numTypes = VarInt.readVInt(is);
        header.setNumberOfTypes(numTypes);

        return header;
    }

    /**
     * Map of string header tags reading.
     *
     * @param dis
     * @throws IOException
     */
    private Map<String, String> readHeaderTags(DataInputStream dis) throws IOException {
        int numHeaderTags = dis.readShort();
        Map<String, String> headerTags = new HashMap<String, String>();
        for (int i = 0; i < numHeaderTags; i++) {
            headerTags.put(dis.readUTF(), dis.readUTF());
        }
        return headerTags;
    }

}
