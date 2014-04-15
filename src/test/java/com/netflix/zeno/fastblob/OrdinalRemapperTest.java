package com.netflix.zeno.fastblob;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.netflix.zeno.fastblob.record.ByteDataBuffer;
import com.netflix.zeno.fastblob.record.FastBlobDeserializationRecord;
import com.netflix.zeno.fastblob.record.FastBlobSerializationRecord;
import com.netflix.zeno.fastblob.record.VarInt;
import com.netflix.zeno.fastblob.record.schema.FastBlobSchema;
import com.netflix.zeno.fastblob.record.schema.FieldDefinition;
import com.netflix.zeno.fastblob.record.schema.FastBlobSchema.FieldType;
import com.netflix.zeno.fastblob.record.schema.TypedFieldDefinition;
import com.netflix.zeno.fastblob.state.OrdinalRemapper;
import com.netflix.zeno.serializer.NFTypeSerializer;
import com.netflix.zeno.serializer.SerializerFactory;
import com.netflix.zeno.serializer.common.ListSerializer;
import com.netflix.zeno.serializer.common.MapSerializer;
import com.netflix.zeno.serializer.common.SetSerializer;
import com.netflix.zeno.testpojos.TypeA;
import com.netflix.zeno.testpojos.TypeASerializer;
import com.netflix.zeno.testpojos.TypeD;
import com.netflix.zeno.testpojos.TypeDSerializer;

/*
 * This test validates only the ordinal remapping functionality.
 *
 * Essentially, we set up a fake mapping for TypeA POJO instances for the LazyBlobSerializer:
 *
 * 0->4 (TypeA(0, 0) -> TypeA(4, 4))
 * 1->3 (TypeA(1, 1) -> TypeA(3, 3))
 * 2->2 (TypeA(2, 2) -> TypeA(2, 2))
 * 3->1 (TypeA(3, 3) -> TypeA(1, 1))
 * 4->0 (TypeA(4, 4) -> TypeA(0, 0))
 *
 * Then we serialize each of the field types which requires remapping.  When we remap the ordinals, then
 * deserialize the transformed data, we expect that the resultant objects will be transformed accordingly, e.g.:
 *
 *  List (TypeA(0, 0)) becomes List(TypeA(4, 4))
 *  Set (TypeA(3, 3), TypeA(0, 0)) -> Set(TypeA(1, 1), TypeA(4, 4))
 *
 *  In actual use, the context for this mapping would be based on reassignment
 *  in the lazy blob, instead of the arbitrary values assigned here for testing.
 *
 */
public class OrdinalRemapperTest {

    private FastBlobStateEngine stateEngine;
    private OrdinalRemapper ordinalRemapper;

    @Before
    public void setUp() {
        stateEngine = new FastBlobStateEngine(new SerializerFactory() {
            public NFTypeSerializer<?>[] createSerializers() {
                return new NFTypeSerializer<?> [] {
                        new ListSerializer<TypeA>("ListOfTypeA", new TypeASerializer()),
                        new SetSerializer<TypeA>("SetOfTypeA", new TypeASerializer()),
                        new MapSerializer<TypeA, TypeA>("MapOfTypeA", new TypeASerializer(), new TypeASerializer()),
                        new TypeDSerializer()
                };
            }
        });

        stateEngine.add("TypeA", new TypeA(0, 0)); // ordinal 0
        stateEngine.add("TypeA", new TypeA(1, 1)); // ordinal 1
        stateEngine.add("TypeA", new TypeA(2, 2)); // ordinal 2
        stateEngine.add("TypeA", new TypeA(3, 3)); // ordinal 3
        stateEngine.add("TypeA", new TypeA(4, 4)); // ordinal 4
        stateEngine.add("TypeA", new TypeA(5, 5)); // ordinal 5
        stateEngine.add("TypeA", new TypeA(6, 6)); // ordinal 6

        /// fill deserialization state from serialization state
        stateEngine.getTypeSerializationState("TypeA").fillDeserializationState(stateEngine.getTypeDeserializationState("TypeA"));


        ByteDataBuffer buf = new ByteDataBuffer();
        
        Map<String, Map<Integer, Integer>> stateOrdinalMappers = new HashMap<String, Map<Integer, Integer>>();
        HashMap<Integer, Integer> hashMap = new HashMap<Integer, Integer>();
        stateOrdinalMappers.put("TypeA", hashMap);
        hashMap.put(0, 4);
        hashMap.put(1, 3);
        hashMap.put(2, 2);
        hashMap.put(3, 1);
        hashMap.put(4, 0);
        hashMap.put(5, 6);
        hashMap.put(6, 5);

        ordinalRemapper = new OrdinalRemapper(stateOrdinalMappers);
    }


    @Test
    public void remapsListOrdinals() {
        NFTypeSerializer<List<TypeA>> listSerializer = stateEngine.getSerializer("ListOfTypeA");
        FastBlobSchema listSchema = listSerializer.getFastBlobSchema();
        FastBlobSerializationRecord rec = new FastBlobSerializationRecord(listSchema);
        rec.setImageMembershipsFlags(new boolean[] { true });

        List<TypeA> list = new ArrayList<TypeA>();
        list.add(new TypeA(3, 3));
        list.add(new TypeA(0, 0));
        list.add(null);
        list.add(new TypeA(2, 2));
        list.add(new TypeA(6, 6));
        list.add(new TypeA(5, 5));

        listSerializer.serialize(list, rec);

        FastBlobDeserializationRecord deserializationRec = createDeserializationRecord(listSchema, rec);

        FastBlobDeserializationRecord remappedRec = remapOrdinals(listSchema, deserializationRec);

        List<TypeA> typeAs = listSerializer.deserialize(remappedRec);

        Assert.assertEquals(6, typeAs.size());
        Assert.assertEquals(new TypeA(1, 1), typeAs.get(0));
        Assert.assertEquals(new TypeA(4, 4), typeAs.get(1));
        Assert.assertNull(typeAs.get(2));
        Assert.assertEquals(new TypeA(2, 2), typeAs.get(3));
    }
    
    @Test
    public void remapsListFieldOrdinals() {
        FastBlobSchema schema = new FastBlobSchema("Test", 2);
        schema.addField("intField", new FieldDefinition(FieldType.INT));
        schema.addField("listField", new TypedFieldDefinition(FieldType.LIST, "ElementType"));
        
        Random rand = new Random();
        
        for(int i=0;i<1000;i++) {
            
            int numElements = rand.nextInt(1000);
            
            
            Map<String, Map<Integer, Integer>> ordinalMapping = new HashMap<String, Map<Integer, Integer>>();
            
            ByteDataBuffer buf = new ByteDataBuffer();

            Map<Integer, Integer> elementOrdinalMap = new HashMap<Integer, Integer>();
            for(int j=0;j<numElements;j++) {
                elementOrdinalMap.put(j, rand.nextInt(10000));
                VarInt.writeVInt(buf, j);
            }
            
            ordinalMapping.put("ElementType", elementOrdinalMap);
            

            ByteDataBuffer fromDataBuffer = new ByteDataBuffer();
            VarInt.writeVInt(fromDataBuffer, 100);
            VarInt.writeVInt(fromDataBuffer, (int)buf.length());
            
            fromDataBuffer.copyFrom(buf);
            
            FastBlobDeserializationRecord rec = new FastBlobDeserializationRecord(schema, fromDataBuffer.getUnderlyingArray());
            rec.position(0);
            
            ByteDataBuffer toDataBuffer = new ByteDataBuffer();
            new OrdinalRemapper(ordinalMapping).remapOrdinals(rec, toDataBuffer);
            
            Assert.assertEquals(100, VarInt.readVInt(toDataBuffer.getUnderlyingArray(), 0));
            
            int listDataSize = VarInt.readVInt(toDataBuffer.getUnderlyingArray(), 1);

            long position = VarInt.sizeOfVInt(listDataSize) + 1;
            long endPosition = position + listDataSize;
            
            int counter = 0;
            
            while(position < endPosition) {
                int encoded = VarInt.readVInt(toDataBuffer.getUnderlyingArray(), position);
                position += VarInt.sizeOfVInt(encoded);
                Assert.assertEquals(encoded, elementOrdinalMap.get(counter).intValue());
                System.out.println(counter + ": " + elementOrdinalMap.get(counter).intValue());
                counter++;
            }
        }
    }

    @Test
    public void remapsSetOrdinals() {
        NFTypeSerializer<Set<TypeA>> setSerializer = stateEngine.getSerializer("SetOfTypeA");
        FastBlobSchema setSchema = setSerializer.getFastBlobSchema();
        FastBlobSerializationRecord rec = new FastBlobSerializationRecord(setSchema);
        rec.setImageMembershipsFlags(new boolean[] { true });

        Set<TypeA> set = new HashSet<TypeA>();
        set.add(new TypeA(1, 1));
        set.add(new TypeA(4, 4));
        set.add(null);

        setSerializer.serialize(set, rec);

        FastBlobDeserializationRecord deserializationRec = createDeserializationRecord(setSchema, rec);

        FastBlobDeserializationRecord remappedRec = remapOrdinals(setSchema, deserializationRec);

        Set<TypeA> typeAs = setSerializer.deserialize(remappedRec);

        Assert.assertEquals(3, typeAs.size());
        Assert.assertTrue(typeAs.contains(new TypeA(3, 3)));
        Assert.assertTrue(typeAs.contains(new TypeA(0, 0)));
        Assert.assertTrue(typeAs.contains(null));
    }

    @Test
    public void remapsMapOrdinals() {
        NFTypeSerializer<Map<TypeA, TypeA>> mapSerializer = stateEngine.getSerializer("MapOfTypeA");
        FastBlobSchema mapSchema = mapSerializer.getFastBlobSchema();
        FastBlobSerializationRecord rec = new FastBlobSerializationRecord(mapSchema);
        rec.setImageMembershipsFlags(new boolean[] { true });

        Map<TypeA, TypeA> map = new HashMap<TypeA, TypeA>();
        map.put(new TypeA(1, 1), new TypeA(4, 4));
        map.put(new TypeA(3, 3), new TypeA(1, 1));
        map.put(null, new TypeA(1, 1));
        map.put(new TypeA(4, 4), null);

        mapSerializer.serialize(map, rec);

        FastBlobDeserializationRecord deserializationRec = createDeserializationRecord(mapSchema, rec);

        FastBlobDeserializationRecord remappedRec = remapOrdinals(mapSchema, deserializationRec);

        Map<TypeA, TypeA> typeAs = mapSerializer.deserialize(remappedRec);

        Assert.assertEquals(4, typeAs.size());
        Assert.assertEquals(new TypeA(0, 0), typeAs.get(new TypeA(3, 3)));
        Assert.assertEquals(new TypeA(3, 3), typeAs.get(new TypeA(1, 1)));
        Assert.assertEquals(new TypeA(3, 3), typeAs.get(null));
        Assert.assertEquals(null, typeAs.get(new TypeA(0, 0)));
    }

    @Test
    public void remapsObjectOrdinals() {
        NFTypeSerializer<TypeD> serializer = stateEngine.getSerializer("TypeD");
        FastBlobSchema typeDSchema = serializer.getFastBlobSchema();
        FastBlobSerializationRecord rec = new FastBlobSerializationRecord(typeDSchema);
        rec.setImageMembershipsFlags(new boolean[] { true });

        TypeD typeD = new TypeD(100, new TypeA(3, 3));

        serializer.serialize(typeD, rec);

        FastBlobDeserializationRecord deserializationRec = createDeserializationRecord(typeDSchema, rec);

        FastBlobDeserializationRecord remappedRec = remapOrdinals(typeDSchema, deserializationRec);

        TypeD deserializedTypeD = serializer.deserialize(remappedRec);

        Assert.assertEquals(Integer.valueOf(100), deserializedTypeD.getVal());
        Assert.assertEquals(new TypeA(1, 1), deserializedTypeD.getTypeA());
    }

    private FastBlobDeserializationRecord createDeserializationRecord(FastBlobSchema schema, FastBlobSerializationRecord rec) {
        ByteDataBuffer originalRecord  = new ByteDataBuffer();
        rec.writeDataTo(originalRecord);
        FastBlobDeserializationRecord deserializationRec = new FastBlobDeserializationRecord(schema, originalRecord.getUnderlyingArray());
        deserializationRec.position(0);
        return deserializationRec;
    }

    private FastBlobDeserializationRecord remapOrdinals(FastBlobSchema schema, FastBlobDeserializationRecord deserializationRec) {
        ByteDataBuffer remappedRecordSpace = new ByteDataBuffer();
        ordinalRemapper.remapOrdinals(deserializationRec, remappedRecordSpace);
        FastBlobDeserializationRecord remappedRec = new FastBlobDeserializationRecord(schema, remappedRecordSpace.getUnderlyingArray());
        remappedRec.position(0);
        return remappedRec;
    }

}