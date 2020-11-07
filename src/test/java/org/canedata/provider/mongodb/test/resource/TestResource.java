package org.canedata.provider.mongodb.test.resource;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoIterable;
import org.canedata.provider.mongodb.test.AbilityProvider;
import org.junit.Test;

/**
 * @author Sun Yitao sunyitao@outlook.com
 * Created at 2020-09-13 01:24
 */
public class TestResource extends AbilityProvider {
    @Test
    public void testGetMongo(){
        MongoClient client = resProvider.getMongo();
        client.listDatabaseNames().forEach(n -> {
            System.out.println(n);
        });

        assert client != null;
    }
}
