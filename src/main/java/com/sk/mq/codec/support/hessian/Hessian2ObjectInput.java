package com.sk.mq.codec.support.hessian;


import com.caucho.hessian.io.Hessian2Input;
import com.sk.mq.codec.ObjectInput;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Type;


public class Hessian2ObjectInput implements ObjectInput {
    private final Hessian2Input hessian2Input;

    public Hessian2ObjectInput(InputStream is) {
        hessian2Input = new Hessian2Input(is);
        hessian2Input.setSerializerFactory(Hessian2SerializerFactory.SERIALIZER_FACTORY);
    }

    @Override
    public boolean readBool() throws IOException {
        return hessian2Input.readBoolean();
    }

    @Override
    public byte readByte() throws IOException {
        return (byte) hessian2Input.readInt();
    }

    @Override
    public short readShort() throws IOException {
        return (short) hessian2Input.readInt();
    }

    @Override
    public int readInt() throws IOException {
        return hessian2Input.readInt();
    }

    @Override
    public long readLong() throws IOException {
        return hessian2Input.readLong();
    }

    @Override
    public float readFloat() throws IOException {
        return (float) hessian2Input.readDouble();
    }

    @Override
    public double readDouble() throws IOException {
        return hessian2Input.readDouble();
    }

    @Override
    public byte[] readBytes() throws IOException {
        return hessian2Input.readBytes();
    }

    @Override
    public String readUTF() throws IOException {
        return hessian2Input.readString();
    }

    @Override
    public Object readObject() throws IOException {
        return hessian2Input.readObject();
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T readObject(Class<T> cls) throws IOException, ClassNotFoundException {
        return (T) hessian2Input.readObject(cls);
    }

    @Override
    public <T> T readObject(Class<T> cls, Type type) throws IOException, ClassNotFoundException {
        return readObject(cls);
    }

}