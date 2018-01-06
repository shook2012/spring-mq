package com.sk.mq.codec;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public interface Serializer {
	
    byte getContentTypeId();

    String getContentType();

    ObjectOutput serialize(OutputStream output) throws IOException;

    ObjectInput deserialize(InputStream input) throws IOException;

}
