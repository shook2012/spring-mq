package com.sk.mq.codec.support.hessian;


import com.caucho.hessian.io.Hessian2Output;
import com.sk.mq.codec.ObjectOutput;

import java.io.IOException;
import java.io.OutputStream;


public class Hessian2ObjectOutput implements ObjectOutput {
    private final Hessian2Output hessian2Output;

    public Hessian2ObjectOutput(OutputStream os) {
        hessian2Output = new Hessian2Output(os);
        hessian2Output.setSerializerFactory(Hessian2SerializerFactory.SERIALIZER_FACTORY);
    }

    @Override
    public void writeBool(boolean v) throws IOException {
        hessian2Output.writeBoolean(v);
    }

    @Override
    public void writeByte(byte v) throws IOException {
        hessian2Output.writeInt(v);
    }

    @Override
    public void writeShort(short v) throws IOException {
        hessian2Output.writeInt(v);
    }

    @Override
    public void writeInt(int v) throws IOException {
        hessian2Output.writeInt(v);
    }

    @Override
    public void writeLong(long v) throws IOException {
        hessian2Output.writeLong(v);
    }

    @Override
    public void writeFloat(float v) throws IOException {
        hessian2Output.writeDouble(v);
    }

    @Override
    public void writeDouble(double v) throws IOException {
        hessian2Output.writeDouble(v);
    }

    @Override
    public void writeBytes(byte[] b) throws IOException {
        hessian2Output.writeBytes(b);
    }

    @Override
    public void writeBytes(byte[] b, int off, int len) throws IOException {
        hessian2Output.writeBytes(b, off, len);
    }

    @Override
    public void writeUTF(String v) throws IOException {
        hessian2Output.writeString(v);
    }

    @Override
    public void writeObject(Object obj) throws IOException {
        hessian2Output.writeObject(obj);
    }

    @Override
    public void flushBuffer() throws IOException {
        hessian2Output.flushBuffer();
    }
}