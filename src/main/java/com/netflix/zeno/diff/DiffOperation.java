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

import com.netflix.zeno.fastblob.FastBlobStateEngine;
import com.netflix.zeno.serializer.SerializerFactory;
import java.util.ArrayList;
import java.util.List;

public class DiffOperation {

    private final SerializerFactory serializerFactory;
    private final DiffInstruction instruction;

    /**
     * Instantiate a DiffOperation, capable of performing a diff between two data states.
     *
     * @param dataModel - The SerializerFactory describing the data model to use.
     * @param instruction - the details about how to find top level objects in a data state
     */
    public DiffOperation(SerializerFactory dataModel, DiffInstruction instruction) {
        this.serializerFactory = dataModel;
        this.instruction = instruction;
    }


    /**
     * Perform a diff between two data states.
     *
     * Note:  For now, this operation will ignore type instructions for non-unique keys.
     *
     * @param fromState - The "from" state engine, populated with one of the deserialized data states to compare
     * @param toState - the "to" state engine, populated with the other deserialized data state to compare.
     * @param factory - The SerializerFactory describing the data model to use.
     * @return the DiffReport for investigation of the differences between the two data states.
     * @throws DiffReportGenerationException
     */
    public DiffReport performDiff(FastBlobStateEngine fromState, FastBlobStateEngine toState) throws DiffReportGenerationException {
        return performDiff(null, fromState, toState);
    }

    public DiffReport performDiff(DiffHeader diffHeader, final FastBlobStateEngine fromState, final FastBlobStateEngine toState) throws DiffReportGenerationException {
        try {
            final List<TypeDiff<?>> diffs = new ArrayList<TypeDiff<?>>();

            final DiffSerializationFramework framework = new DiffSerializationFramework(serializerFactory);

            for (final TypeDiffInstruction<?> instruction : this.instruction.getTypeInstructions()) {
                /// for now, the DiffOperation ignores non-unique keys.
                if(instruction.isUniqueKey()) {
                    Iterable<?> fromDeserializationState = fromState.getTypeDeserializationState(instruction.getSerializerName());
                    Iterable<?> toDeserializationState = toState.getTypeDeserializationState(instruction.getSerializerName());

                    TypeDiff<Object> typeDiff = performDiff(framework, instruction, fromDeserializationState, toDeserializationState);
                    diffs.add(typeDiff);
                }
            }

            return new DiffReport(diffHeader, diffs);
        } catch (Exception e) {
            throw new DiffReportGenerationException(e);
        }
    }

    @SuppressWarnings("unchecked")
    private <T> TypeDiff<T> performDiff(DiffSerializationFramework framework, TypeDiffInstruction<?> diff, Iterable<?> from, Iterable<?> to) {
        TypeDiffInstruction<T> castDiff = (TypeDiffInstruction<T>) diff;
        Iterable<T> castFrom = (Iterable<T>) from;
        Iterable<T> castTo = (Iterable<T>) to;

        return new TypeDiffOperation<T>(castDiff).performDiff(framework, castFrom, castTo, Runtime.getRuntime().availableProcessors());
    }

}
