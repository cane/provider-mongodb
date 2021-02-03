package org.canedata.provider.mongodb.test.tx;

import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.TransactionOptions;
import com.mongodb.WriteConcern;
import com.mongodb.client.ClientSession;
import org.canedata.entity.Entity;
import org.canedata.field.Fields;
import org.canedata.provider.mongodb.entity.Options;
import org.canedata.provider.mongodb.test.AbilityProvider;
import org.canedata.ta.Transaction;
import org.canedata.ta.TransactionException;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * @author Sun Yitao sunyitao@outlook.com
 * Created at 2020-11-07 17:10
 */
public class TestTx extends AbilityProvider  {
    private static final String ENTITY = "user";
    static TransactionOptions txnOptions = null;

    @BeforeClass
    public static void setup() {
        txnOptions = TransactionOptions.builder()
                .readPreference(ReadPreference.primary())
                .readConcern(ReadConcern.MAJORITY)
                .writeConcern(WriteConcern.MAJORITY)
                .build();
    }


    public void txOption() {
        Entity e = factory.get(ENTITY);
        e.opt(Options.TRANSACTION_OPTIONS, txnOptions);
    }

    public void tx() {
        ClientSession session = mongo.startSession();
        session.startTransaction(txnOptions);
        session.commitTransaction();
        //session.abortTransaction();
    }

    @Test
    public void etx() {
        Entity e = factory.get(ENTITY);
        e.opt(Options.TRANSACTION_OPTIONS, txnOptions);

        String id = "tx-test-1";
        String id2 = "tx-test-2";

        try(Transaction ta = e.transaction();){
            e.put("name", "tx name").create(id);
            e.put("name", "tx name 2").create(id2);

            //ta.commit();

            Fields rlt = e.findOne(e.expr().equals("_id", id));
            Assert.assertNotNull(rlt);
            Fields rlt2 = e.findOne(e.expr().equals("_id", id2));
            Assert.assertNotNull(rlt2);
        }

    }
}
