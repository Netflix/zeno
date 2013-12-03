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
import java.util.HashMap;
import java.util.Map;

/**
 * A bunch of primitive serializers with corresponding array serializers
 * @author tvaliulin
 */
class HashGenericRecordSerializers {
    public static interface Serializer {
        void serialize(HashAlgorithm hasher, Object obj) throws IOException;
    }

    static Map<Class<?>, Serializer> serializers = new HashMap<Class<?>, Serializer>();
    static Map<Class<?>, Serializer> primitiveArraySerializers = new HashMap<Class<?>, Serializer>();

    // Constructing static serializers per type
    static {
        {
            Serializer stringSerializer = new Serializer() {
                @Override
                public void serialize(HashAlgorithm hasher, Object obj) throws IOException {
                    hasher.write((String) obj);
                }
            };
            serializers.put(String.class, stringSerializer);

            Serializer doubleSerializer = new Serializer() {
                @Override
                public void serialize(HashAlgorithm hasher, Object obj) throws IOException {
                    hasher.write((Double) obj);
                }
            };
            serializers.put(Double.class, doubleSerializer);
            serializers.put(Double.TYPE, doubleSerializer);

            Serializer floatSerializer = new Serializer() {
                @Override
                public void serialize(HashAlgorithm hasher, Object obj) throws IOException {
                    hasher.write((Float) obj);
                }
            };
            serializers.put(Float.class, floatSerializer);
            serializers.put(Float.TYPE, floatSerializer);

            Serializer longSerializer = new Serializer() {
                @Override
                public void serialize(HashAlgorithm hasher, Object obj) throws IOException {
                    hasher.write((Long) obj);
                }
            };
            serializers.put(Long.class, longSerializer);
            serializers.put(Long.TYPE, longSerializer);

            Serializer integerSerializer = new Serializer() {
                @Override
                public void serialize(HashAlgorithm hasher, Object obj) throws IOException {
                    hasher.write((Integer) obj);
                }
            };
            serializers.put(Integer.class, integerSerializer);
            serializers.put(Integer.TYPE, integerSerializer);

            Serializer shortSerializer = new Serializer() {
                @Override
                public void serialize(HashAlgorithm hasher, Object obj) throws IOException {
                    hasher.write((Short) obj);
                }
            };
            serializers.put(Short.class, shortSerializer);
            serializers.put(Short.TYPE, shortSerializer);

            Serializer byteSerializer = new Serializer() {
                @Override
                public void serialize(HashAlgorithm hasher, Object obj) throws IOException {
                    hasher.write((Byte) obj);
                }
            };
            serializers.put(Byte.class, byteSerializer);
            serializers.put(Byte.TYPE, byteSerializer);

            Serializer booleanSerializer = new Serializer() {
                @Override
                public void serialize(HashAlgorithm hasher, Object obj) throws IOException {
                    hasher.write((Boolean) obj);
                }
            };
            serializers.put(Boolean.class, booleanSerializer);
            serializers.put(Boolean.TYPE, booleanSerializer);

            Serializer characterSerializer = new Serializer() {
                @Override
                public void serialize(HashAlgorithm hasher, Object obj) throws IOException {
                    hasher.write((Character) obj);
                }
            };
            serializers.put(Character.class, characterSerializer);
            serializers.put(Character.TYPE, characterSerializer);
        }

        {
            Serializer doubleSerializer = new Serializer() {
                @Override
                public void serialize(HashAlgorithm hasher, Object obj) throws IOException {
                    hasher.write((double[]) obj);
                }
            };
            primitiveArraySerializers.put(Double.TYPE, doubleSerializer);

            Serializer floatSerializer = new Serializer() {
                @Override
                public void serialize(HashAlgorithm hasher, Object obj) throws IOException {
                    hasher.write((float[]) obj);
                }
            };
            primitiveArraySerializers.put(Float.TYPE, floatSerializer);

            Serializer longSerializer = new Serializer() {
                @Override
                public void serialize(HashAlgorithm hasher, Object obj) throws IOException {
                    hasher.write((long[]) obj);
                }
            };
            primitiveArraySerializers.put(Long.TYPE, longSerializer);

            Serializer integerSerializer = new Serializer() {
                @Override
                public void serialize(HashAlgorithm hasher, Object obj) throws IOException {
                    hasher.write((int[]) obj);
                }
            };
            primitiveArraySerializers.put(Integer.TYPE, integerSerializer);

            Serializer shortSerializer = new Serializer() {
                @Override
                public void serialize(HashAlgorithm hasher, Object obj) throws IOException {
                    hasher.write((short[]) obj);
                }
            };
            primitiveArraySerializers.put(Short.TYPE, shortSerializer);

            Serializer byteSerializer = new Serializer() {
                @Override
                public void serialize(HashAlgorithm hasher, Object obj) throws IOException {
                    hasher.write((byte[]) obj);
                }
            };
            primitiveArraySerializers.put(Byte.TYPE, byteSerializer);

            Serializer booleanSerializer = new Serializer() {
                @Override
                public void serialize(HashAlgorithm hasher, Object obj) throws IOException {
                    hasher.write((boolean[]) obj);
                }
            };
            primitiveArraySerializers.put(Boolean.TYPE, booleanSerializer);

            Serializer characterSerializer = new Serializer() {
                @Override
                public void serialize(HashAlgorithm hasher, Object obj) throws IOException {
                    hasher.write((char[]) obj);
                }
            };
            primitiveArraySerializers.put(Character.TYPE, characterSerializer);
        }
    }

    public static Serializer getTypeSerializer(Class<?> type) {
        return serializers.get(type);
    }

    public static Serializer getPrimitiveArraySerializer(Class<?> type) {
        return primitiveArraySerializers.get(type);
    }
}
