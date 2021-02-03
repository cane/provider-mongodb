/*
 * Copyright (c) 2013 CaneData.org
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.canedata.provider.mongodb.test;

import com.mongodb.*;
import com.mongodb.client.MongoCollection;
import org.canedata.entity.Entity;
import org.canedata.field.Fields;
import org.canedata.provider.mongodb.entity.MongoEntity;
import org.canedata.provider.mongodb.entity.Options;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Method;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;

/**
 * @author Sun Yat-ton
 * @version 1.00.000 2013-09-29
 */
public class TestOptions extends AbilityProvider {
    private static final String ENTITY = "user";

    @Before
    public void setup() {
        initData();
    }

    @Test
    public void findAndUpdateAndReturnNew() throws Exception {
        Entity e = factory.get(ENTITY);
        try {
            Fields neu = e.put("age", 18).put("name", "r new").opt(Options.RETURN_NEW, true).findOneAndUpdate(e.expr().equals("_id", "id:test:1"));

            assertEquals(neu.getString("_id"), "id:test:1");
            assertEquals(neu.getInt("age"), 18);
            assertEquals(neu.getString("name"), "r new");
        } finally {
            if (null != e)
                e.close();
        }

    }

    @Test
    public void upsert() throws Exception {
        Entity e = factory.get(ENTITY);
        String id = "test:upsert-is-update-or-insert";

        try{
            e.put("name", "upsert").put("age", 22).opt(Options.UPSERT, true).update(id);
            Fields rlt = e.restore(id);
            assertEquals(rlt.getString("name"), "upsert");
            assertEquals(rlt.getInt("age"), 22);
        }finally {
            if(null != e)
                e.close();
        }
    }


    public void collectionOptions() throws Exception {
        Entity e = factory.get(ENTITY);
        ReadPreference rp = ReadPreference.secondary();
        WriteConcern wc = WriteConcern.JOURNALED;
        ReadConcern rc = ReadConcern.LINEARIZABLE;

        BasicDBObject options = new BasicDBObject();
        options.append(Options.READ_PREFERENCE, rp)
        .append(Options.READ_CONCERN, rc)
        .append(Options.WRITE_CONCERN, wc);

        Method m = MongoEntity.class.getDeclaredMethod("prepareCollection", BasicDBObject.class);
        m.setAccessible(true);
        MongoCollection dbc = (MongoCollection)m.invoke(e, options);

        assertEquals(dbc.getReadPreference(), rp);
        assertEquals(dbc.getReadConcern(), rc);
        assertEquals(dbc.getWriteConcern(), wc);
    }
}
