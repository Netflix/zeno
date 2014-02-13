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

import java.util.HashMap;
import java.util.Map;

/**
 * Represents the fast blob header
 *
 * @author plakhina
 *
 */
public class FastBlobHeader {

    public static final int FAST_BLOB_VERSION_HEADER = 1029;

    private String version = "";
    private Map<String, String> headerTags = new HashMap<String, String>();
    private int deserializationBufferSizeHint;
    private int numberOfTypes;

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public Map<String, String> getHeaderTags() {
        return headerTags;
    }

    public void setHeaderTags(Map<String, String> headerTags) {
        this.headerTags = headerTags;
    }

    public int getDeserializationBufferSizeHint() {
        return deserializationBufferSizeHint;
    }

    public void setDeserializationBufferSizeHint(int deserializationBufferSizeHint) {
        this.deserializationBufferSizeHint = deserializationBufferSizeHint;
    }

    public int getNumberOfTypes() {
        return numberOfTypes;
    }

    public void setNumberOfTypes(int numberOfTypes) {
        this.numberOfTypes = numberOfTypes;
    }

    @Override
    public boolean equals(Object other) {
        if(other instanceof FastBlobHeader) {
            FastBlobHeader oh = (FastBlobHeader)other;
            return version.equals(oh.getVersion()) &&
                    headerTags.equals(oh.getHeaderTags()) &&
                    deserializationBufferSizeHint == oh.getDeserializationBufferSizeHint() &&
                    numberOfTypes == oh.getNumberOfTypes();
        }
        return false;
    }
}
