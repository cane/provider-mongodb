package org.canedata.provider.mongodb.codecs;

import org.bson.BsonReader;
import org.bson.BsonType;
import org.bson.BsonWriter;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;

import java.util.Vector;

/**
 * @author Sun Yitao sunyitao@outlook.com
 * Created at 2020-09-21 14:42
 */
public class StringsCodec implements Codec<String[]> {
    @Override
    public String[] decode(BsonReader reader, DecoderContext decoderContext) {
        reader.readStartArray();
        Vector<String> list = new Vector<>();

        while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
            list.add(reader.readString());
        }

        reader.readEndArray();
        String[] r = list.toArray(new String[]{});
        return r;
    }

    @Override
    public void encode(BsonWriter writer, String[] ss, EncoderContext encoderContext) {
        writer.writeStartArray();

        for (String s : ss) {
            writer.writeString(s);
        }

        writer.writeEndArray();
    }

    @Override
    public Class<String[]> getEncoderClass() {
        return String[].class;
    }
}
