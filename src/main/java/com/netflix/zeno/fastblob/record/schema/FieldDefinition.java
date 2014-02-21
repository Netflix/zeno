package com.netflix.zeno.fastblob.record.schema;

import com.netflix.zeno.fastblob.record.schema.FastBlobSchema.FieldType;

public class FieldDefinition {
    private final FieldType fieldType;

    public FieldDefinition(FieldType fieldType) {
        this.fieldType = fieldType;
    }

    public FieldType getFieldType() {
        return fieldType;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result
                + ((fieldType == null) ? 0 : fieldType.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        FieldDefinition other = (FieldDefinition) obj;
        if (fieldType != other.fieldType)
            return false;
        return true;
    }

}
