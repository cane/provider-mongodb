package org.canedata.provider.mongodb.codecs;

import org.bson.BsonBinary;
import org.bson.BsonReader;
import org.bson.BsonWriter;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;

import java.math.BigInteger;

/**
 * @author Sun Yitao sunyitao@outlook.com
 * Created at 2020-09-18 10:55
 */
public class BigIntegerCodec implements Codec<BigInteger> {

    @Override
    public BigInteger decode(BsonReader bsonReader, DecoderContext decoderContext) {
        return new BigInteger(bsonReader.readBinaryData().getData());
        //return BigInteger.valueOf(bsonReader.readInt64());
    }

    @Override
    public void encode(BsonWriter bsonWriter, BigInteger bigInteger, EncoderContext encoderContext) {
        byte[] s = bigInteger.toByteArray();
        bsonWriter.writeBinaryData(new BsonBinary(s));
        //bsonWriter.wr.writeInt64(bigInteger.longValue());
    }

    @Override
    public Class<BigInteger> getEncoderClass() {
        return BigInteger.class;
    }
}
