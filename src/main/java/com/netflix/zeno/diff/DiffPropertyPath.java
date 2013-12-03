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

import com.netflix.zeno.serializer.NFTypeSerializer;

import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The diff operation must flatten out an object hierarchy into a set of key/value pairs.  The keys are
 * represented by the "path" of properties, (i.e. sequence of {@link NFTypeSerializer} fields)  which must 
 * be traversed in order to arrive at the value.<p/>
 * 
 * This class describes a path through the object hierarchy to a property, starting with a top level serializer.<p/>
 *
 * @author dkoszewnik
 *
 */
public class DiffPropertyPath implements Cloneable, Comparable<DiffPropertyPath> {

    private static final ConcurrentHashMap<DiffPropertyPath, DiffPropertyPath> canonicalDiffBreadcrumbs = new ConcurrentHashMap<DiffPropertyPath, DiffPropertyPath>();

    private String topNodeSerializer;
    private final String fieldBreadcrumbs[];
    private int breadcrumbLength;
    private int hashCode;

    public DiffPropertyPath() {
        this.fieldBreadcrumbs = new String[256];
        this.breadcrumbLength = 0;
    }

    private DiffPropertyPath(DiffPropertyPath copy) {
        this.topNodeSerializer = copy.topNodeSerializer;
        this.fieldBreadcrumbs = copy.getFieldBreadcrumbsCopy();
        this.breadcrumbLength = fieldBreadcrumbs.length;
        this.hashCode = copy.hashCode;
    }

    DiffPropertyPath(String topNodeSerializer, String fieldBreadcrumbs[]) {
        this.topNodeSerializer = topNodeSerializer;
        this.fieldBreadcrumbs = fieldBreadcrumbs;
        this.breadcrumbLength = fieldBreadcrumbs.length;
    }

    String getTopNodeSerializer() {
        return topNodeSerializer;
    }

    String[] getBreadcrumbArray() {
        return fieldBreadcrumbs;
    }

    int getBreadcrumbLength() {
        return breadcrumbLength;
    }

    public void setTopNodeSerializer(String topNodeSerializer) {
        hashCode = 0;
        this.topNodeSerializer = topNodeSerializer;
    }

    public void addBreadcrumb(String field) {
        if(hashCode != 0)
            hashCode ^= breadcrumbLength * field.hashCode();
        fieldBreadcrumbs[breadcrumbLength] = field;
        breadcrumbLength++;
    }

    public void removeBreadcrumb() {
        breadcrumbLength--;
        if(hashCode != 0)
            hashCode ^= (breadcrumbLength) * fieldBreadcrumbs[breadcrumbLength].hashCode();
    }

    public void reset() {
        hashCode = 0;
        breadcrumbLength = 0;
    }

    public DiffPropertyPath copy() {
        DiffPropertyPath copy = canonicalDiffBreadcrumbs.get(this);
        if(copy == null) {
            DiffPropertyPath newCopy = new DiffPropertyPath(this);
            copy = canonicalDiffBreadcrumbs.putIfAbsent(newCopy, newCopy);
            if(copy == null)
                copy = newCopy;
        }
        return copy;
    }

    @Override
    public boolean equals(Object anotherObject) {
        if(anotherObject instanceof DiffPropertyPath) {
            DiffPropertyPath other = (DiffPropertyPath)anotherObject;
            if(other.breadcrumbLength == this.breadcrumbLength) {
                if(other.topNodeSerializer.equals(this.topNodeSerializer)) {
                    for(int i=breadcrumbLength - 1; i >= 0; i--) {
                        if(!other.fieldBreadcrumbs[i].equals(this.fieldBreadcrumbs[i]))
                            return false;
                    }
                    return true;
                }

            }
        }
        return false;
    }

    @Override
    public int hashCode() {
        if(hashCode != 0)
            return hashCode;

        int result = 1 + 32 * (topNodeSerializer.hashCode());

        for(int i=0;i<breadcrumbLength;i++) {
            result ^= i * fieldBreadcrumbs[i].hashCode();
        }

        hashCode = result;

        return result;
    }

    @Override
    public int compareTo(DiffPropertyPath o) {
        int comp = this.topNodeSerializer.compareTo(o.topNodeSerializer);
        if(comp == 0) {
            comp = breadcrumbLength - o.breadcrumbLength;
            for(int i=breadcrumbLength - 1;i >= 0 && comp == 0;i--) {
                comp = this.fieldBreadcrumbs[i].compareTo(o.fieldBreadcrumbs[i]);
            }
        }

        return comp;
    }

    private String[] getFieldBreadcrumbsCopy() {
        return Arrays.copyOf(fieldBreadcrumbs, breadcrumbLength);
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder(topNodeSerializer);

        for(int i=0;i<breadcrumbLength;i++) {
            builder.append(".");
            builder.append(fieldBreadcrumbs[i]);
        }

        return builder.toString();
    }

}
