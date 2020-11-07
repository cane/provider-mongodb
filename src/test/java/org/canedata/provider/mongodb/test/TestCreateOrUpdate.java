package org.canedata.provider.mongodb.test;

import org.bson.Document;
import org.canedata.field.Fields;
import org.canedata.provider.mongodb.entity.MongoEntity;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Sun Yitao sunyitao@outlook.com
 * Created at 2020-09-18 18:26
 */
public class TestCreateOrUpdate extends AbilityProvider {
    @Before
    public void setup() {
        clear();
    }

    @Test
    public void notExisted(){
        MongoEntity e = factory.get("user");
        Fields f = e.put("newField", "new")
                .put("name", "new test")
                .put("$inc", new Document().append("inc", 1))
                .createOrUpdate("abcd:1233");

        assertTrue("new".equals(f.get("newField")));
        assertTrue("abcd:1233".equals(f.get("_id")));
    }

    @Test
    public void existed(){
        MongoEntity e = factory.get("user");
        Fields f = e.put("newField", "new")
                .put("name", "new test")
                .createOrUpdate("id:test:1");

        assertTrue("new".equals(f.get("newField")));
        assertTrue("id:test:1".equals(f.get("_id")));
    }

    @Test
    public void mapKey() {
        Map<String, Object> keys = new HashMap<String, Object>(){{put("_id", "id:map:key");}};

        MongoEntity e = factory.get("user");
        Fields f = e.put("newField", "new")
                .put("name", "new test")
                .createOrUpdate(keys);

        assertTrue("new".equals(f.get("newField")));
        System.out.println(f.getString("_id"));
        assertTrue("id:map:key".equals(f.getString("_id")));
    }
}
