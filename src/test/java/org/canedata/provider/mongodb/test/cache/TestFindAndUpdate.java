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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.bson.Document;
import org.canedata.entity.Entity;
import org.canedata.field.Fields;
import org.canedata.provider.mongodb.entity.Options;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/**
 * 
 * @author Sun Yat-ton
 * @version 1.00.000 2011-9-2
 */
public class TestFindAndUpdate extends CacheAbilityProvider {
	@Before
	public void setup() {
		initData();
	}

	@Test
	public void byKey() {
		Entity e = factory.get("user");
		assertNotNull(e);


		String id = "id:test:3";
		Fields f = e.restore(id);
		assertTrue(f.getInt("age") == 13);
		assertTrue(!f.isRestored());
		
		Fields upd = e.projection("age").opt(Options.RETURN_NEW, false).put("age", 19).findOneAndUpdate(e.filter().equals("_id", id));
		assertFalse(upd.isRestored());
		assertEquals(upd.getInt("age"), 13);
		
		f = e.restore(id);
		assertTrue(f.getInt("age") == 19);
		assertTrue(!f.isRestored());
	}
	@Test
	public void t() {
		Entity e = factory.get("user");
		assertNotNull(e);

		/*List<Fields> rlt = e.filter(e.expr().equals("_id", "id:test:3")).list();
		rlt.forEach(i -> {
			System.out.println(i.unwrap(Document.class).toJson());
		});*/
	}
	
}
