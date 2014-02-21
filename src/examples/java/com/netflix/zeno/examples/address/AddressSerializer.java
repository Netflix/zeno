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
package com.netflix.zeno.examples.address;

import com.netflix.zeno.fastblob.record.schema.FastBlobSchema;
import com.netflix.zeno.fastblob.record.schema.FastBlobSchema.FieldType;
import com.netflix.zeno.serializer.NFDeserializationRecord;
import com.netflix.zeno.serializer.NFSerializationRecord;
import com.netflix.zeno.serializer.NFTypeSerializer;

import java.util.Collection;
import java.util.Collections;

/**
 * This class is here for demonstration purposes.  It is a serializer for a less efficient representation of an Address
 * object.  Follow along with:  https://github.com/Netflix/zeno/wiki/Designing-for-efficiency to see how
 * to reason about how to make Zeno more effective at deduplicating your object model.
 *
 * @author dkoszewnik
 *
 */
public class AddressSerializer extends NFTypeSerializer<Address> {

    public AddressSerializer() {
        super("Address");
    }

    @Override
    public void doSerialize(Address value, NFSerializationRecord rec) {
        serializePrimitive(rec, "street", value.getStreetAddress());
        serializePrimitive(rec, "city", value.getCity());
        serializePrimitive(rec, "state", value.getState());
        serializePrimitive(rec, "postalCode", value.getPostalCode());
    }

    @Override
    protected Address doDeserialize(NFDeserializationRecord rec) {
        String streetAddress = deserializePrimitiveString(rec, "street");
        String city = deserializePrimitiveString(rec, "city");
        String state = deserializePrimitiveString(rec, "state");
        String postalCode = deserializePrimitiveString(rec, "postalCode");

        return new Address(streetAddress, city, state, postalCode);
    }

    @Override
    protected FastBlobSchema createSchema() {
        return schema(
                field("street", FieldType.STRING),
                field("city", FieldType.STRING),
                field("state", FieldType.STRING),
                field("postalCode", FieldType.STRING)
        );
    }

    @Override
    public Collection<NFTypeSerializer<?>> requiredSubSerializers() {
        return Collections.emptyList();
    }

}
