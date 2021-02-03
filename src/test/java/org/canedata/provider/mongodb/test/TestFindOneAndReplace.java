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

import org.canedata.field.Fields;
import org.canedata.provider.mongodb.entity.MongoEntity;
import org.canedata.provider.mongodb.entity.Options;
import org.junit.Before;
import org.junit.Test;

import static junit.framework.Assert.*;

/**
 * @author Sun Yat-ton
 * @version 1.00.000 2013-09-29
 */
public class TestFindOneAndReplace extends AbilityProvider {
    private static final String ENTITY = "user";
    private static final String ID = "id:test:1";


    @Before
    public void setup() {
        initData();
    }

    @Test
    public void update() {
        Thread t1 = new Thread(new Runnable() {
            public void run() {
                MongoEntity e = factory.get(ENTITY);
                Fields rlt = e.put("age", 17).opt(Options.RETURN_NEW, true).findOneAndReplace(e.expr().equals("name", "mongo"));
                assertNotNull(rlt);
                assertEquals(rlt.getInt("age"), 17);

            }
        });
        Thread t2 = new Thread(new Runnable() {
            public void run() {
                MongoEntity e = factory.get(ENTITY);
                Fields rlt = e.put("age", 18).opt(Options.RETURN_NEW, true).findOneAndReplace(e.expr().equals("name", "mongo"));
                assertNotNull(rlt);
                assertEquals(rlt.getInt("age"), 18);

            }
        });
        Thread t3 = new Thread(new Runnable() {
            public void run() {
                MongoEntity e = factory.get(ENTITY);
                Fields rlt = e.put("age", 19).opt(Options.RETURN_NEW, true).findOneAndReplace(e.expr().equals("name", "mongo"));
                assertEquals(rlt.getInt("age"), 19);

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
        MongoEntity e = factory.get(ENTITY);

        Fields rlt = e.opt(Options.RETURN_NEW, true).put("age", "14").findOneAndReplace(e.expr().equals("_id", "id:test:2"));
        assertNotNull(rlt);
        assertEquals(rlt.getInt("age"), 14);
    }
}
