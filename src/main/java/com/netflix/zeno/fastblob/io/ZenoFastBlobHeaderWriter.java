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

import com.netflix.zeno.fastblob.FastBlobStateEngine;
import com.netflix.zeno.fastblob.record.VarInt;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Map;

public class ZenoFastBlobHeaderWriter implements FastBlobHeaderWriter {

    public void writeHeader(FastBlobHeader header, FastBlobStateEngine stateEngine, DataOutputStream dos) throws IOException {
        /// save 4 bytes to indicate FastBlob version header.  This will be changed to indicate backwards incompatibility.
        dos.writeInt(FastBlobHeader.FAST_BLOB_VERSION_HEADER);

        /// write the version from the state engine
        dos.writeUTF(header.getVersion());

        /// write the header tags -- intended to include input source data versions
        dos.writeShort(header.getHeaderTags().size());

        for (Map.Entry<String, String> headerTag : header.getHeaderTags().entrySet()) {
            dos.writeUTF(headerTag.getKey());
            dos.writeUTF(headerTag.getValue());
        }

        dos.write(header.getDeserializationBufferSizeHint());

        /// flags byte -- reserved for later use
        dos.write(0);

        VarInt.writeVInt(dos, header.getNumberOfTypes());
    }

}
