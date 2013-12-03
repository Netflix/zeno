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
package com.netflix.zeno.diff;

import java.util.Arrays;

import org.apache.commons.codec.binary.Base64;

/**
 *
 * Wrapper around a byte array, necessary to implement equals() and hashCode().
 *
 * @author dkoszewnik
 *
 */
public class DiffByteArray {

    private final byte bytes[];

    public DiffByteArray(byte bytes[]) {
        this.bytes = bytes;
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(bytes);
    }

    @Override
    public boolean equals(Object obj) {
        if(obj instanceof DiffByteArray) {
            return Arrays.equals(bytes, ((DiffByteArray) obj).bytes);
        }
        return false;
    }

    @Override
    public String toString() {
        return Base64.encodeBase64String(bytes);
    }



}
