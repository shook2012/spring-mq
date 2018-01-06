package com.sk.mq.codec.support.nativejava;


import com.sk.mq.codec.ObjectInput;
import com.sk.mq.codec.ObjectOutput;
import com.sk.mq.codec.Serializer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;


public class NativeJavaSerialization implements Serializer {

    public static final String NAME = "nativejava";

    @Override
    public byte getContentTypeId() {
        return 7;
    }

    @Override
    public String getContentType() {
        return "x-application/nativejava";
    }

    @Override
    public ObjectOutput serialize(OutputStream output) throws IOException {
        return new NativeJavaObjectOutput(output);
    }

    @Override
    public ObjectInput deserialize(InputStream input) throws IOException {
        return new NativeJavaObjectInput(input);
    }
}
