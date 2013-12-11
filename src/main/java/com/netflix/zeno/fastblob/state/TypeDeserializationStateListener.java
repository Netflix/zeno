package com.netflix.zeno.fastblob.state;

public abstract class TypeDeserializationStateListener<T> {

    public abstract void removedObject(T obj);

    public abstract void addedObject(T obj);

    private static final TypeDeserializationStateListener<Object> NOOP_CALLBACK =
            new TypeDeserializationStateListener<Object>() {
                @Override
                public void removedObject(Object obj) { }

                @Override
                public void addedObject(Object obj) { }
            };

    @SuppressWarnings("unchecked")
    public static <T> TypeDeserializationStateListener<T> noopCallback() {
        return (TypeDeserializationStateListener<T>) NOOP_CALLBACK;
    }

}
