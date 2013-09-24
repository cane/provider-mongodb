/**
 * Copyright 2011 Jlue.org
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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.canedata.entity.Entity;
import org.canedata.field.Fields;
import org.junit.Test;

/**
 * Test restore entity from cache.
 * 
 * @author Sun Yat-ton (Mail:ImSunYitao@gmail.com)
 * @version 1.00.000 2011-9-2
 */
public class TestRestore extends CacheAbilityProvider {
	@Test
	public void restore() {
		Entity e = factory.get("user");
		assertNotNull(e);

		String id = "id:test:1";
		e.put("age", 13).update(id);//invalidate cache
		
		Fields f = e.restore(id);
		assertFalse(f.isRestored());
		assertEquals(f.getInt("age"), 13);
		assertEquals(f.getString("4up"), null);
		assertEquals(f.getString("_id"), id);

		f = e.restore(id);
		assertTrue(f.isRestored());
		assertEquals(f.getInt("age"), 13);
		assertEquals(f.getString("4up"), null);
		assertEquals(f.getString("_id"), id);

		e.close();
	}
	
	@Test
	public void restoreByProj() {
		Entity e = factory.get("user");
		assertNotNull(e);

		Fields f = e.projection("age", "4up").restore("id:test:1");
		assertEquals(f.getInt("age"), 13);
		assertEquals(f.getString("4up"), null);
		assertNull(f.get("_id"));
		assertNull(f.get("4inc"));

		f = e.restore("id:test:1");
		assertEquals(f.getInt("age"), 13);
		assertEquals(f.getString("4up"), null);
		assertEquals(f.getString("_id"), "id:test:1");

		e.close();
	}
}
