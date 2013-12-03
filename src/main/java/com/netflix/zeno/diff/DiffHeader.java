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

/**
 * Header fields for a {@link DiffReport}
 *
 * @author dkoszewnik
 *
 */
public class DiffHeader {

    private final String vip1;
    private final String vip2;
    private final String blob1;
    private final String blob2;
    private final String version1;
    private final String version2;

    public DiffHeader(String vip1, String blob1, String version1, String vip2, String blob2, String version2) {
        this.vip1 = vip1;
        this.vip2 = vip2;
        this.blob1 = blob2;
        this.blob2 = blob2;
        this.version1 = version1;
        this.version2 = version2;
    }

    public String getVip1() {
        return vip1;
    }

    public String getVip2() {
        return vip2;
    }

    public String getBlob1() {
        return blob1;
    }

    public String getBlob2() {
        return blob2;
    }

    public String getVersion1() {
        return version1;
    }

    public String getVersion2() {
        return version2;
    }

    public String getFormattedfromString() {
        return String.format("%s-%s-%s", vip1, blob1, version1);
    }

    public String getFormattedToString() {
        return String.format("%s-%s-%s", vip2, blob2, version2);
    }
}
