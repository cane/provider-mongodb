package org.canedata.provider.mongodb.test.cache;

import org.bson.Document;
import org.canedata.field.Fields;
import org.canedata.provider.mongodb.entity.MongoEntity;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Sun Yitao sunyitao@outlook.com
 * Created at 2020-09-19 15:10
 */
public class TestCreate4Cache extends CacheAbilityProvider {
    @Before
    public void setup() {
        clear();
    }

    @Test
    public void notExisted(){
        MongoEntity e = factory.get("user");

        String key = "abcd:1233" + Math.random();
        Fields f = e.put("newField", "new")
                .put("name", "new test")
                .put("$inc", new Document().append("inc", 1))
                .createOrUpdate(key);

        assert "new".equals(f.get("newField"));
        assert key.equals(f.get("_id"));
    }

    @Test
    public void existed(){
        MongoEntity e = factory.get("user");
        Fields f = e.put("newField", "new")
                .put("name", "new test")
                .createOrUpdate("id:test:1");

        assert "new".equals(f.get("newField"));
        assert "id:test:1".equals(f.get("_id"));
    }

    @Test
    public void mapKey() {
        Map<String, Object> keys = new HashMap<String, Object>(){{put("_id", "id:map:key");}};

        MongoEntity e = factory.get("user");
        Fields f = e.put("newField", "new")
                .put("name", "new test")
                .createOrUpdate(keys);

        assert "new".equals(f.get("newField"));
        assert "id:map:key".equals(f.getString("_id"));
    }
}
