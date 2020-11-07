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
import org.canedata.entity.Entity;
import org.canedata.expression.Expression;
import org.canedata.field.Fields;
import org.canedata.provider.mongodb.entity.MongoEntity;
import org.canedata.provider.mongodb.entity.Options;
import org.junit.Before;
import org.junit.Test;

import static junit.framework.Assert.*;

/**
 * @author Sun Yitao
 * @version 1.00.000 2020-09-19
 */
public class TestFindOneAndDelete extends AbilityProvider {
    private static final String ENTITY = "user";
    private static final String ID = "id:test:1";

    @Before
    public void setup() {
        initData();
    }

    @Test
    public void delete() {
        MongoEntity e = factory.get(ENTITY);
        Expression expr = e.expr().equals("name", "mongo");
        Fields rlt = e.findOneAndDelete(expr);
        assertNotNull(rlt);
        assertEquals(rlt.getInt("age"), 19);

        assert !e.filter(expr).exists("_id");
    }

}
