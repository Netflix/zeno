package com.netflix.zeno.flatblob;

import com.netflix.zeno.serializer.FrameworkSerializer;
import com.netflix.zeno.serializer.NFTypeSerializer;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

public class FlatBlobEvictionFrameworkSerializer extends FrameworkSerializer<FlatBlobSerializationRecord> {

    private final FlatBlobSerializationFramework flatBlobFramework;

    public FlatBlobEvictionFrameworkSerializer(FlatBlobEvictor evictor, FlatBlobSerializationFramework flatBlobFramework) {
        super(evictor);
        this.flatBlobFramework = flatBlobFramework;
    }

    @Override
    public void serializePrimitive(FlatBlobSerializationRecord rec, String fieldName, Object value) {
        /// nothing to do.
    }

    @Override
    public void serializeBytes(FlatBlobSerializationRecord rec, String fieldName, byte[] value) {
        /// nothing to do.
    }

    /*
     * @Deprecated instead use serializeObject(NFSerializationRecord rec, String fieldName, Object obj)
     * 
     */
    @Override
    @Deprecated
    @SuppressWarnings("unchecked")
    public void serializeObject(FlatBlobSerializationRecord rec, String fieldName, String typeName, Object obj) {
        getSerializer(typeName).serialize(obj, rec);
        flatBlobFramework.getTypeCache(typeName).evict(obj);
    }

    @Override
    public void serializeObject(FlatBlobSerializationRecord rec, String fieldName, Object obj) {
        serializeObject(rec, fieldName, rec.getObjectType(fieldName), obj);
    }
     

    @Override
    @SuppressWarnings("unchecked")
    public <T> void serializeList(FlatBlobSerializationRecord rec, String fieldName, String typeName, Collection<T> obj) {
        FlatBlobTypeCache<T> typeCache = flatBlobFramework.getTypeCache(typeName);
        NFTypeSerializer<T> serializer = getSerializer(typeName);
        for(T t : obj) {
            serializer.serialize(t, rec);
            typeCache.evict(t);
        }
    }

    @Override
    public <T> void serializeSet(FlatBlobSerializationRecord rec, String fieldName, String typeName, Set<T> obj) {
        serializeList(rec, fieldName, typeName, obj);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <K, V> void serializeMap(FlatBlobSerializationRecord rec, String fieldName, String keyTypeName, String valueTypeName, Map<K, V> obj) {
        FlatBlobTypeCache<K> keyCache = flatBlobFramework.getTypeCache(keyTypeName);
        FlatBlobTypeCache<V> valueCache = flatBlobFramework.getTypeCache(valueTypeName);
        NFTypeSerializer<K> keySerializer = getSerializer(keyTypeName);
        NFTypeSerializer<V> valueSerializer = getSerializer(valueTypeName);

        for(Map.Entry<K, V> entry : obj.entrySet()) {
            keySerializer.serialize(entry.getKey(), rec);
            keyCache.evict(entry.getKey());
            valueSerializer.serialize(entry.getValue(), rec);
            valueCache.evict(entry.getValue());
        }
    }

}
