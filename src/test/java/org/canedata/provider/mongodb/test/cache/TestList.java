/**
 * Copyright 2011 CaneData.org
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
package org.canedata.provider.mongodb.test.cache;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.client.model.Updates;
import org.bson.Document;
import org.canedata.entity.Entity;
import org.canedata.field.Fields;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Test restore entity from cache.
 * 
 * @author Sun Yat-ton
 * @version 1.00.000 2011-9-2
 */
public class TestList extends CacheAbilityProvider {
	@Before
	public void setup() {
		initData();
	}

	@Test
	public void list() {

		Entity e = factory.get("list");
		assertNotNull(e);

		String id = "id:test:1";

		//e.put("f", new String[]{"a","b"}).createOrUpdate(id);//invalidate cache
		List<String> dbList = new ArrayList<>();

		dbList.add("a");
		dbList.add("b");
		e.put("c", new BasicDBObject().append("key", "value"));
		e.put("f", dbList).createOrUpdate(id);


		
		Fields f = e.restore(id);
		assertTrue(!f.isRestored());
        ArrayList b = (ArrayList)f.get("f");
		assertEquals(b.get(0), "a");
        assertEquals(b.get(1), "b");

        assert f.get("c") instanceof Document;
	}
}
