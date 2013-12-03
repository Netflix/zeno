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
package com.netflix.zeno.fastblob.state;

import java.util.Iterator;
import java.util.List;

/**
 *
 * Iterator over the List containing a FastBlobTypeDeserializationState's Objects.<p/>
 *
 * This implementation skips null elements and does not support remove().
 *
 * @author dkoszewnik
 *
 */
public class TypeDeserializationStateIterator<T> implements Iterator<T> {

    private final List<T> list;
    private int currentOrdinal = 0;

    public TypeDeserializationStateIterator(List<T> stateList) {
        this.list = stateList;
        this.currentOrdinal = -1;
        moveToNext();
    }

    @Override
    public boolean hasNext() {
        return currentOrdinal < list.size();
    }

    @Override
    public T next() {
        T current = list.get(currentOrdinal);
        moveToNext();
        return current;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

    private void moveToNext() {
        currentOrdinal++;
        while(currentOrdinal < list.size()) {
            if(list.get(currentOrdinal) != null)
                return;
            currentOrdinal++;
        }
    }
}
