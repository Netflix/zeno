package com.netflix.zeno.fastblob.lazy.schema;

import com.netflix.zeno.fastblob.record.ByteData;
import com.netflix.zeno.fastblob.record.ByteDataBuffer;
import com.netflix.zeno.fastblob.record.FastBlobDeserializationRecord;
import com.netflix.zeno.fastblob.record.FastBlobSerializationRecord;
import com.netflix.zeno.fastblob.record.schema.FastBlobSchema;
import com.netflix.zeno.fastblob.record.schema.FieldDefinition;

import java.util.HashMap;
import java.util.Map;

public class CommonSchemaReconciler {

    public static FastBlobSchema deriveCommonSchema(FastBlobSchema schema1, FastBlobSchema schema2) {
        Map<String, FieldDefinition> commonFieldDefinitions = new HashMap<String, FieldDefinition>();

        for(int i=0;i<schema1.numFields();i++) {
            String fieldName = schema1.getFieldName(i);
            FieldDefinition def1 = schema1.getFieldDefinition(i);
            int schema2Position = schema2.getPosition(fieldName);

            if(schema2Position != -1) {
                FieldDefinition def2 = schema2.getFieldDefinition(schema2Position);

                if(!def1.equals(def2)) {
                    throw new RuntimeException("Schemas are incompatible!  Field " + fieldName + " has unequal types!");
                }

                commonFieldDefinitions.put(fieldName, def1);
            }
        }

        FastBlobSchema commonSchema = new FastBlobSchema(schema1.getName(), commonFieldDefinitions.size());

        for(Map.Entry<String, FieldDefinition>entry : commonFieldDefinitions.entrySet()) {
            commonSchema.addField(entry.getKey(), entry.getValue());
        }

        return commonSchema;
    }

    public static void swapRecordSchema(FastBlobDeserializationRecord fromRecord, FastBlobSchema toSchema, ByteDataBuffer toSpace) {
        ByteData fromSpace = fromRecord.getByteData();

        for(int i=0;i<toSchema.numFields();i++) {
            String fieldName = toSchema.getFieldName(i);
            int fromSchemaPosition = fromRecord.getSchema().getPosition(fieldName);

            if(fromSchemaPosition != -1) {
                long fieldPosition = fromRecord.getPosition(fromSchemaPosition);
                int fieldLength = fromRecord.getFieldLength(fromSchemaPosition);

                toSpace.copyFrom(fromSpace, fieldPosition, fieldLength);
            } else {
                FastBlobSerializationRecord.writeNullField(toSpace, toSchema, i);
            }
        }
    }

}
