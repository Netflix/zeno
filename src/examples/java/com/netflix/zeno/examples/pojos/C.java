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
package com.netflix.zeno.examples.pojos;

public class C {

    private final long cLong;
    private final byte cBytes[];

    public C(long cLong, byte cBytes[]) {
        this.cLong = cLong;
        this.cBytes = cBytes;
    }

    public long getCLong() {
        return cLong;
    }

    public byte[] getCBytes() {
        return cBytes;
    }

}
