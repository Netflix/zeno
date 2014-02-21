package com.netflix.zeno.fastblob.record.schema;

import com.netflix.zeno.fastblob.record.schema.FastBlobSchema.FieldType;

public class MapFieldDefinition extends FieldDefinition {
    private final String keyType;
    private final String valueType;

    public MapFieldDefinition(String keyType, String valueType) {
        super(FieldType.MAP);
        this.keyType = keyType;
        this.valueType = valueType;
    }

    public String getKeyType() {
        return keyType;
    }

    public String getValueType() {
        return valueType;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result + ((keyType == null) ? 0 : keyType.hashCode());
        result = prime * result
                + ((valueType == null) ? 0 : valueType.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (!super.equals(obj))
            return false;
        if (getClass() != obj.getClass())
            return false;
        MapFieldDefinition other = (MapFieldDefinition) obj;
        if (keyType == null) {
            if (other.keyType != null)
                return false;
        } else if (!keyType.equals(other.keyType))
            return false;
        if (valueType == null) {
            if (other.valueType != null)
                return false;
        } else if (!valueType.equals(other.valueType))
            return false;
        return true;
    }


}