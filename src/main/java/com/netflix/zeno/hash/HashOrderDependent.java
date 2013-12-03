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

import java.io.DataOutputStream;
import java.io.IOException;
import java.security.DigestOutputStream;
import java.security.MessageDigest;

import org.apache.commons.io.output.NullOutputStream;

/**
 * Implements hashing algorithm which is order dependent
 *
 * @author tvaliulin
 *
 */
public class HashOrderDependent implements HashAlgorithm
{
    MessageDigest digest;
    DigestOutputStream digestOutputStream;
    DataOutputStream dataOutputStream;

    public HashOrderDependent(){
        try{
            digest = MessageDigest.getInstance("MD5");
        } catch ( Exception ex ) {
            throw new RuntimeException(ex);
        }
        digestOutputStream = new DigestOutputStream(NullOutputStream.NULL_OUTPUT_STREAM, digest);
        dataOutputStream = new DataOutputStream(digestOutputStream);
    }

    @Override
    public void write(char b) throws IOException {
        dataOutputStream.write(b);
    }

    @Override
    public void write(boolean b) throws IOException {
        dataOutputStream.writeBoolean(b);
    }

    @Override
    public void write(long b) throws IOException {
        dataOutputStream.writeLong(b);
    }

    @Override
    public void write(float b) throws IOException {
        dataOutputStream.writeFloat(b);
    }

    @Override
    public void write(double b) throws IOException {
        dataOutputStream.writeDouble(b);
    }

    @Override
    public void write(String b) throws IOException {
        dataOutputStream.writeUTF(b);
    }

    @Override
    public void write(byte[] b) throws IOException {
        dataOutputStream.write(b);
    }

    @Override
    public void write(char[] b) throws IOException {
        for(char c : b){
            dataOutputStream.writeChar(c);
        }
    }

    @Override
    public void write(boolean[] b) throws IOException {
        for(boolean c : b){
            dataOutputStream.writeBoolean(c);
        }
    }

    @Override
    public void write(short[] b) throws IOException {
        for(short c : b){
            dataOutputStream.writeShort(c);
        }
    }

    @Override
    public void write(int[] b) throws IOException {
        for(int c : b){
            dataOutputStream.writeInt(c);
        }
    }

    @Override
    public void write(long[] b) throws IOException {
        for(long c : b){
            dataOutputStream.writeLong(c);
        }
    }

    @Override
    public void write(float[] b) throws IOException {
        for(float c : b){
            dataOutputStream.writeFloat(c);
        }
    }

    @Override
    public void write(double[] b) throws IOException {
        for(double c : b){
            dataOutputStream.writeDouble(c);
        }
    }

    @Override
    public byte[] bytes()
    {
        try {
            digestOutputStream.close();
        } catch (IOException e) {
           throw new RuntimeException(e);
        }
        return digest.digest();
    }

}
