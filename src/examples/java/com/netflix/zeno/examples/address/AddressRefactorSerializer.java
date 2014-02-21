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

import com.netflix.zeno.examples.address.AddressRefactor.City;
import com.netflix.zeno.fastblob.record.schema.FastBlobSchema;
import com.netflix.zeno.fastblob.record.schema.FastBlobSchema.FieldType;
import com.netflix.zeno.serializer.NFDeserializationRecord;
import com.netflix.zeno.serializer.NFSerializationRecord;
import com.netflix.zeno.serializer.NFTypeSerializer;
import com.netflix.zeno.serializer.common.StringSerializer;

import java.util.Collection;

/**
 * This class is here for demonstration purposes.  It is a serializer for a more efficient representation of an Address
 * object.  Follow along with:  https://github.com/Netflix/zeno/wiki/Designing-for-efficiency to see how
 * to reason about how to make Zeno more effective at deduplicating your object model.
 *
 * @author dkoszewnik
 *
 */

public class AddressRefactorSerializer extends NFTypeSerializer<AddressRefactor> {

    public AddressRefactorSerializer() {
        super("Address");
    }

    @Override
    public void doSerialize(AddressRefactor value, NFSerializationRecord rec) {
        serializePrimitive(rec, "street", value.getStreetAddress());
        serializeObject(rec, "city", value.getCity());
        serializeObject(rec, "postalCode", value.getPostalCode());
    }

    @Override
    protected AddressRefactor doDeserialize(NFDeserializationRecord rec) {
        String streetAddress = deserializePrimitiveString(rec, "street");
        City city = deserializeObject(rec, "city");
        String postalCode = deserializeObject(rec, "postalCode");

        return new AddressRefactor(streetAddress, city, postalCode);
    }

    @Override
    protected FastBlobSchema createSchema() {
        return schema(
                field("street", FieldType.STRING),
                field("city", "City"),
                field("postalCode", "PostalCodeString")
        );
    }

    @Override
    public Collection<NFTypeSerializer<?>> requiredSubSerializers() {
        return serializers(
                new CitySerializer(),
                new StringSerializer("PostalCodeString")
        );
    }

}
