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
package org.canedata.provider.mongodb.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

import org.canedata.entity.Entity;
import org.canedata.field.Fields;
import org.junit.Test;

/**
 * 
 * @author Sun Yat-ton
 * @version 1.00.000 2011-9-2
 */
public class TestLast extends AbilityProvider {
	@Test
	public void last() {
		Entity e = factory.get("user");
		assertNotNull(e);
		
		Fields f = e.filter(e.expr().notLike("name", "cane")).order("age").last();
		assertFalse(f.isRestored());
		assertEquals(f.getInt("age"), 63);
		assertEquals(f.getString("4up"), null);
		
		e.close();
	}
}
