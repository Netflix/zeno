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
import com.netflix.zeno.serializer.NFTypeSerializer;
import com.netflix.zeno.serializer.SerializerFactory;

/**
 * The DiffInstruction describes how to derive a {@link DiffReport} on the deserialized Objects
 * contained in two {@link FastBlobStateEngines}.<p/>
 *
 * In order to perform a diff, we must be able to match up equivalent Objects at the roots
 * of the FastBlobStateEngine.  For each type we want included in the diff report, we must
 * specify a {@link TypeDiffInstruction}.  Each TypeDiffInstruction informs how to match
 * up individual elements of that type.  Each pair of Objects will be examined for differences
 * throughout the hierarchy defined by the {@link NFTypeSerializer}s.
 *
 * @author dkoszewnik
 *
 */
public class DiffInstruction {

    private final TypeDiffInstruction<?> instructionList[];

    public DiffInstruction(TypeDiffInstruction<?>... instructions) {
        instructionList = instructions;
    }

    public TypeDiffInstruction<?> getTypeInstruction(String topNodeSerializer) {
        for (TypeDiffInstruction<?> instruction : instructionList) {
            if (instruction.getSerializerName().equals(topNodeSerializer)) {
                return instruction;
            }
        }
        return null;
    }

    public TypeDiffInstruction<?>[] getTypeInstructions() {
        return instructionList;
    }

    /**
     * @deprecated instead use the interface provided by {@link DiffOperation} 
     */
    @Deprecated
    public DiffReport performDiff(FastBlobStateEngine fromState, FastBlobStateEngine toState, SerializerFactory factory) throws DiffReportGenerationException {
        return performDiff(null, fromState, toState, factory);
    }

    /**
     * @deprecated instead use the interface provided by {@link DiffOperation} 
     */
    public DiffReport performDiff(DiffHeader diffHeader, final FastBlobStateEngine fromState, final FastBlobStateEngine toState, SerializerFactory factory) throws DiffReportGenerationException {
        return new DiffOperation(factory, this).performDiff(diffHeader, fromState, toState);
    }

}
