/**
 * Copyright 2011 CaneData.org
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.canedata.provider.mongodb.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import org.bson.Document;
import org.canedata.entity.Entity;
import org.canedata.provider.mongodb.entity.MongoEntity;
import org.canedata.provider.mongodb.expr.MongoExpressionBuilder;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

/**
 *
 * @author Sun Yat-ton
 * @version 1.00.000 2011-9-19
 */
public class TestFilter extends AbilityProvider {
    @Before
    public void setup() {
        initData();
    }

    @Test
    public void multiOr() {
        Entity e = factory.get("user");
        assertNotNull(e);

        int c = e
                .projection("name")
                .filter(e.expr().notEquals("name", "cane").or()
                        .notEquals("4up", "cane").or()
                        .notEquals("vendor", "cane")).count().intValue();
        assertEquals(c, 9);

        e.close();
    }

    @Test
    public void or_and_or() {
        Entity e = factory.get("user");
        assertNotNull(e);

        int c = e
                .projection("name")
                .filter(e.expr().notEquals("name", "cane").and()
                        .notEquals("4up", "cane").or()
                        .notEquals("vendor", "cane")).count().intValue();
        assertEquals(c, 9);

        e.close();
    }

    @Test
    public void or_subOr() {
        Entity e = factory.get("user");
        assertNotNull(e);

        int c = e
                .projection("name")
                .filter(e.expr().notEquals("name", "cane").or(e.expr().notEquals("4up", "cane").or()
                        .notEquals("vendor", "cane"))
                ).count().intValue();
        assertEquals(c, 9);

        e.close();
    }

    @Test
    public void multiSubOr() {
        Entity e = factory.get("user");
        assertNotNull(e);

        int c = e
                .projection("name")
                .filter(e.expr().equals("name", "cane").or(e.expr().equals("4up", "dd")).and()
                        .equals("4up", "dd").or(e.expr().notEquals("4up", "xxx").or().equals("vendor", "cane"))).count().intValue();
        assertEquals(c, 9);

        e.close();
    }

    @Test
    public void th3() {
        Entity e = factory.get("user");
        assertNotNull(e);

        int c = e
                .projection("name")
                .filter(e.expr().notEquals("name", "cane").and(e.expr().notEquals("4up", "cane").or()
                        .notEquals("vendor", "cane"))
                ).count().intValue();
        assertEquals(c, 8);

        e.close();
    }

    /**
     * sub or filter after sub and filter
     */
    @Test
    public void th4() {
        Entity e = factory.get("user");
        assertNotNull(e);

        int c = e
                .projection("name")
                .filter(e.expr().notEquals("name", "cane").and(e.expr().notEquals("4up", "cane").or()
                        .notEquals("vendor", "cane"))
                ).count().intValue();
        assertEquals(c, 8);

        e.close();
    }

    @Test
    public void multiAnd() {
        Entity e = factory.get("user");
        assertNotNull(e);

        int c = e
                .projection("name")
                .filter(e.expr().equals("name", "cane").and().equals("gender", 0).and()
                        .equals("vendor", "cane team")
                ).count().intValue();
        assertEquals(c, 1);

        e.close();
    }

    @Test
    public void multiSubAnd() {
        Entity e = factory.get("user");
        assertNotNull(e);

        int c = e
                .projection("name")
                .filter(e.expr().equals("name", "cane").and(e.expr().equals("gender", 0)).and(e.expr().equals("vendor", "cane team"))
                ).count().intValue();
        assertEquals(c, 1);

        e.close();
    }

    @Test
    public void testAll() {
        MongoEntity e = factory.get("user");
        assertNotNull(e);

        e.put("tags", new String[]{"a1", "b1", "a2"}).create();

        MongoExpressionBuilder exp = (MongoExpressionBuilder) e.filter();
        int c = e.filter(exp.all("tags", "a1", "b1", "a2")).count().intValue();
        assertEquals(c, 1);
    }

    /**
     * db.inventory.find( {
     *                      qty: { $all: [
     *                                     { "$elemMatch" : { size: "M", num: { $gt: 50} } },
     *                                     { "$elemMatch" : { num : 100, color: "green" } }
     *                                   ] }
     *                    } )
     */
    @Test
    public void testAllAndMatch() {
        MongoEntity e = factory.get("user");
        assertNotNull(e);

        e.put("tags", Arrays.asList(new Document().append("age", 11).append("grade", 6))).create();

        MongoExpressionBuilder exp = (MongoExpressionBuilder) e.filter();
        Document matcher = new Document();
        matcher.append("$elemMatch", new Document().append("age", 11).append("grade", new Document().append("$gte", 6)));
        int c = e.filter(exp.all("tags", matcher)).count().intValue();
        assertEquals(c, 1);
    }
}
