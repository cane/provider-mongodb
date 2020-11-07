package org.canedata.provider.mongodb.test;

import org.bson.Document;
import org.canedata.entity.Entity;
import org.canedata.field.Fields;
import org.canedata.provider.mongodb.entity.MongoEntity;
import org.junit.Before;
import org.junit.Test;


/**
 * @author Sun Yitao sunyitao@outlook.com
 * Created at 2020-09-18 09:57
 */
public class TestFindOne extends AbilityProvider {
    private static final String ENTITY = "user";

    @Before
    public void setup() {
        initData();
    }

    @Test
    public void notExisted(){
        MongoEntity e = factory.get(ENTITY);
        Fields f =e.findOne(e.filter().equals("_id", "not existed"));
        assert f == null;

        //Document doc = f.getWrapped(Document.class);
    }
}