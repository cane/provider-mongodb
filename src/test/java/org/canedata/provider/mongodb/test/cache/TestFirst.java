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

import org.canedata.entity.Entity;
import org.canedata.field.Fields;
import org.junit.Test;

/**
 * 
 * @author Sun Yat-ton
 * @version 1.00.000 2011-9-2
 */
public class TestFirst extends CacheAbilityProvider {
	@Test
	public void first() {
		Entity e = factory.get("user");
		assertNotNull(e);
		
		String id = "id:test:1";
		e.put("age", 13).put("4up", "").update(id);//invalidate cache
		
		long tMark = System.currentTimeMillis();
		
		Fields f = e.filter(e.expr().equals("_id", id)).order("_id").first();
		assertNotNull(f);
		assertFalse(f.isRestored());
		assertEquals(f.getInt("age"), 13);
		assertEquals(f.getString("4up"), "");
		
		f = e.filter(e.expr().equals("_id", id)).order("_id").first();
		assertTrue(f.isRestored());
		assertEquals(f.getInt("age"), 13);
		assertEquals(f.getString("4up"), "");
		assertTrue(tMark < f.getCacheTime());
	}
}
