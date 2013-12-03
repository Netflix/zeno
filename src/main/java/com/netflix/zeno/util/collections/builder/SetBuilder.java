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
package com.netflix.zeno.util.collections.builder;

import java.util.Set;

/**
 * Set builder interface which facilitates creation of Sets in serializer
 *
 * @author tvaliulin
 *
 * @param <E>
 */
public interface SetBuilder<E> {
    void builderInit(int size);

    void builderSet(int index, E element);

    Set<E> builderFinish();
}
