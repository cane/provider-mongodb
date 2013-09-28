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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.canedata.entity.Entity;
import org.canedata.field.Fields;
import org.junit.Test;

/**
 * 
 * @author Sun Yat-ton
 * @version 1.00.000 2011-9-2
 */
public class TestDelete extends CacheAbilityProvider {
	@Test
	public void byKey() {
		Entity e = factory.get("user");
		assertNotNull(e);

		String id = "id:test:a";
		Fields f = e.restore(id);
		assertTrue(f.isRestored());

		int c = e.delete(id);
		assertEquals(c, 1);

		f = e.restore(id);
		assertNull(f);

		e.close();
	}

	@Test
	public void byRange() {
		Entity e = factory.get("user");
		assertNotNull(e);

		assertEquals(e.filter(e.expr().equals("age", 13)).count(), 3l);

		int c = e.deleteRange(e.expr().equals("age", 13));
		assertEquals(c, 3);

		assertEquals(e.filter(e.expr().equals("age", 13)).count(), 0l);

		e.close();
	}
}
