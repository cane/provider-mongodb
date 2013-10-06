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

import com.mongodb.BasicDBObject;
import com.mongodb.util.JSON;
import org.canedata.entity.Entity;
import org.canedata.field.Fields;
import org.canedata.provider.mongodb.entity.Options;
import org.junit.Test;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertNull;

/**
 * @author Sun Yat-ton
 * @version 1.00.000 2013-09-29
 */
public class TestFindAndUpdate extends AbilityProvider {
    private static final String ENTITY = "user";
    private static final String ID = "id:test:1";


    @Test
    public void update() {
        Thread t1 = new Thread(new Runnable() {
            public void run() {
                Entity e = factory.get(ENTITY);
                try {
                    Fields rlt = e.put("age", 17).opt(Options.RETURN_NEW, true).findAndUpdate(e.expr().equals("name", "mongo"));
                    assertNotNull(rlt);
                    assertEquals(rlt.getInt("age"), 17);
                } finally {
                    if (null != e)
                        e.close();
                }

            }
        });
        Thread t2 = new Thread(new Runnable() {
            public void run() {
                Entity e = factory.get(ENTITY);
                try {
                    Fields rlt = e.put("age", 18).opt(Options.RETURN_NEW, true).findAndUpdate(e.expr().equals("name", "mongo"));
                    assertNotNull(rlt);
                    assertEquals(rlt.getInt("age"), 18);
                } finally {
                    if (null != e)
                        e.close();
                }

            }
        });
        Thread t3 = new Thread(new Runnable() {
            public void run() {
                Entity e = factory.get(ENTITY);
                try {
                    Fields rlt = e.put("age", 19).opt(Options.RETURN_NEW, true).findAndUpdate(e.expr().equals("name", "mongo"));
                    assertEquals(rlt.getInt("age"), 19);
                } finally {
                    if (null != e)
                        e.close();
                }

            }
        });

        t1.start();
        t2.start();
        t3.start();


        try {
            t1.join();
            t2.join();
            t3.join();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void inc(){
        Entity e = factory.get(ENTITY);

        try{
            Fields rlt = e.opt(Options.RETURN_NEW, true).put("$inc", new BasicDBObject().append("age", 1)).findAndUpdate(e.expr().equals("_id", "id:test:2"));
            assertNotNull(rlt);
            assertEquals(rlt.getInt("age"), 14);
        }finally {
            if(null != e)
                e.close();
        }
    }

    @Test
    public void remove() {
        Thread t1 = new Thread(new Runnable() {
            public void run() {
                Entity e = factory.get(ENTITY);
                try {
                    Fields rlt = e.restore(ID);
                    assertEquals(rlt.getInt("age"), 13);
                } finally {
                    if (null != e)
                        e.close();
                }

            }
        });
        Thread t2 = new Thread(new Runnable() {
            public void run() {
                Entity e = factory.get(ENTITY);
                try {
                    Fields rlt = e.opt(Options.FIND_AND_REMOVE, true).findAndUpdate(e.expr().equals("_id", ID));
                    assertEquals(rlt.getInt("age"), 13);
                } finally {
                    if (null != e)
                        e.close();
                }

            }
        });

        t1.start();
        t2.start();


        try {
            t1.join();
            t2.join();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        Entity e = factory.get(ENTITY);
        try {
            Fields rlt = e.restore(ID);

            assertNull(rlt);
        } finally {
            if (null != e)
                e.close();
        }
    }
}
