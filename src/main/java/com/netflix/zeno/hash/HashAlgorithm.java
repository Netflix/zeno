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
package com.netflix.zeno.hash;

import java.io.IOException;

/**
 * Hashing algorithm interface
 *
 * @author tvaliulin
 */
public interface HashAlgorithm {

    public abstract void write(char b) throws IOException;

    public abstract void write(boolean b) throws IOException;

    public abstract void write(long b) throws IOException;

    public abstract void write(float b) throws IOException;

    public abstract void write(double b) throws IOException;

    public abstract void write(String b) throws IOException;

    public abstract void write(byte[] b) throws IOException;

    public abstract void write(char[] b) throws IOException;

    public abstract void write(boolean[] b) throws IOException;

    public abstract void write(short[] b) throws IOException;

    public abstract void write(int[] b) throws IOException;

    public abstract void write(long[] b) throws IOException;

    public abstract void write(float[] b) throws IOException;

    public abstract void write(double[] b) throws IOException;

    /**
     * @return the hash of the previously written entities
     */
    public abstract byte[] bytes();

}