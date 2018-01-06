package com.sk.mq.codec.support.hessian;



import com.sk.mq.codec.ObjectInput;
import com.sk.mq.codec.ObjectOutput;
import com.sk.mq.codec.Serializer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;


public class Hessian2Serialization implements Serializer {

    public static final byte ID = 2;

    @Override
    public byte getContentTypeId() {
        return ID;
    }

    @Override
    public String getContentType() {
        return "x-application/hessian2";
    }

    @Override
    public ObjectOutput serialize(OutputStream out) throws IOException {
        
        return new Hessian2ObjectOutput(out);
    }

    @Override
    public ObjectInput deserialize(InputStream is) throws IOException {
        
        return new Hessian2ObjectInput(is);
    }

}