package com.netflix.zeno.fastblob.state;

public interface TypeDeserializationState<T> {

    T get(int ordinal);

}
