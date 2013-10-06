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
import org.canedata.entity.Command;
import org.canedata.entity.Entity;
import org.canedata.entity.EntityFactory;
import org.canedata.field.Fields;
import org.canedata.provider.mongodb.MongoResource;
import org.canedata.provider.mongodb.entity.MongoEntity;
import org.canedata.provider.mongodb.entity.Options;
import org.canedata.resource.Resource;
import org.junit.Test;

import java.lang.reflect.Method;
import java.util.List;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;

/**
 * @author Sun Yat-ton
 * @version 1.00.000 2013-09-29
 */
public class TestOptions extends AbilityProvider {
    private static final String ENTITY = "user";

    @Test
    public void findAndUpdateAndReturnNew() throws Exception {
        Entity e = factory.get(ENTITY);
        try {
            Fields neu = e.put("age", 18).put("name", "rnew").opt(Options.RETURN_NEW, true).findAndUpdate(e.expr().equals("_id", "id:test:1"));

            assertEquals(neu.getString("_id"), "id:test:1");
            assertEquals(neu.getInt("age"), 18);
            assertEquals(neu.getString("name"), "rnew");
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

    @Test
    public void findAndRemove() throws Exception {
        Entity e = factory.get(ENTITY);
        String id = "test:findAndRemove";

        try {
            e.put("name", "findAndRemove").put("age", 19).create(id);

            Fields neu = e.opt(Options.FIND_AND_REMOVE, true).findAndUpdate(e.expr().equals("_id", id));

            assertTrue(!e.exists(id));
        } finally {
            if (null != e)
                e.close();
        }
    }

    @Test
    public void mongoOption() throws Exception {
        Entity e = factory.get(ENTITY);

        try{
            try{List<Fields> rlt = e.opt(Options.MONGO_OPTION, -1).list();}catch (Exception e1){}
            e.execute(new Command() {
                public String getName() {
                    return "test";
                }

                public String describe() {
                    return "test for mongo option: read preference";
                }

                public <D> D execute(EntityFactory factory, Resource<?> res, Entity target, Object... args) {
                    DB db = ((MongoResource)res).take();
                    try {
                        Method m = MongoEntity.class.getDeclaredMethod("getCollection");
                        m.setAccessible(true);
                        DBCollection dbc = (DBCollection)m.invoke(target);
                        assertEquals(-1, dbc.getOptions());
                    } catch (Exception e1) {
                        throw new RuntimeException(e1);
                    }
                    return null;
                }
            });


        }finally {
            if(null != e)
                e.close();
        }
    }

    @Test
    public void readPreference() throws Exception {
        Entity e = factory.get(ENTITY);
        ReadPreference rp = ReadPreference.secondary();

        try{
            List<Fields> rlt = e.opt(Options.READ_PREFERENCE, rp).list();
            Object backed = e.execute(new Command() {
                public String getName() {
                    return "test";
                }

                public String describe() {
                    return "test for mongo option: read preference";
                }

                public <D> D execute(EntityFactory factory, Resource<?> res, Entity target, Object... args) {
                    DB db = ((MongoResource)res).take();
                    try {
                        Method m = MongoEntity.class.getDeclaredMethod("getCollection");
                        m.setAccessible(true);
                        DBCollection dbc = (DBCollection)m.invoke(target);
                        return (D)dbc.getReadPreference();
                    } catch (Exception e1) {
                        throw new RuntimeException(e1);
                    }
                }
            });

            assertEquals(rp, backed);
        }finally {
            if(null != e)
                e.close();
        }
    }

    @Test
    public void writeConcern() throws Exception {
        Entity e = factory.get(ENTITY);
        WriteConcern wc = WriteConcern.FSYNC_SAFE;

        try{
            e.put("age", 18).put("name", "name").opt(Options.WRITE_CONCERN, wc).create();
            Object backed = e.execute(new Command() {
                public String getName() {
                    return "test";
                }

                public String describe() {
                    return "test for mongo option: read preference";
                }

                public <D> D execute(EntityFactory factory, Resource<?> res, Entity target, Object... args) {
                    DB db = ((MongoResource)res).take();
                    try {
                        Method m = MongoEntity.class.getDeclaredMethod("getCollection");
                        m.setAccessible(true);
                        DBCollection dbc = (DBCollection)m.invoke(target);
                        return (D)dbc.getWriteConcern();
                    } catch (Exception e1) {
                        throw new RuntimeException(e1);
                    }
                }
            });

            assertEquals(wc, backed);
        }finally {
            if(null != e)
                e.close();
        }
    }
}
