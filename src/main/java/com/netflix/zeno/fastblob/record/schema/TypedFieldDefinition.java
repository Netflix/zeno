package com.netflix.zeno.fastblob.record.schema;

import com.netflix.zeno.fastblob.record.schema.FastBlobSchema.FieldType;

public class TypedFieldDefinition extends FieldDefinition {
    private final String subType;

    public TypedFieldDefinition(FieldType fieldType, String subType) {
        super(fieldType);
        this.subType = subType;
    }

    public String getSubType() {
        return subType;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result + ((subType == null) ? 0 : subType.hashCode());
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
        TypedFieldDefinition other = (TypedFieldDefinition) obj;
        if (subType == null) {
            if (other.subType != null)
                return false;
        } else if (!subType.equals(other.subType))
            return false;
        return true;
    }


}
