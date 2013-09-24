/**
 * Copyright 2012 CaneData.org
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

import java.util.List;

import org.canedata.entity.Entity;
import org.canedata.field.Fields;
import org.junit.Test;

/**
 * 
 * @author Sun Yat-ton
 * @version 1.00.000 2012-2-25
 */
public class TestDistinct extends AbilityProvider {
	@Test
	public void distinct() {
		Entity e = factory.get("user");
		assertNotNull(e);

		List<Fields> rlt = e.distinct("name");
		assertFalse(rlt.isEmpty());
		assertEquals(rlt.size(), 4);

		for (Fields f : rlt) {
			System.out.println(f.getString("name"));
		}
	}

	@Test
	public void distinctByFiltered() {
		Entity e = factory.get("user");
		assertNotNull(e);

		List<Fields> rlt = e.filter(e.expr().greaterThan("age", 13)).distinct(
				"name");
		assertFalse(rlt.isEmpty());
		assertEquals(rlt.size(), 3);

		for (Fields f : rlt) {
			System.out.println(f.getString("name"));
		}
	}
	
	@Test
	public void distinctByExpr() {
		Entity e = factory.get("user");
		assertNotNull(e);

		List<Fields> rlt = e.distinct(
				"name", e.expr().greaterThan("age", 13));
		assertFalse(rlt.isEmpty());
		assertEquals(rlt.size(), 3);

		for (Fields f : rlt) {
			System.out.println(f.getString("name"));
		}
	}
}
