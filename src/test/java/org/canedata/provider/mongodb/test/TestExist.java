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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.bson.types.ObjectId;
import org.canedata.entity.Entity;
import org.canedata.expression.Expression;
import org.canedata.field.Fields;
import org.canedata.provider.mongodb.expr.MongoExpression;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * 
 * @author Sun Yat-ton
 * @version 1.00.000 2011-8-9
 */
public class TestExist extends AbilityProvider {
	static ObjectId id;
	@Before
	public void init(){
		clear();

		Entity e = factory.get("user");
		assertNotNull(e);
	
		Fields f = e.put("name", "cane").put("value", "v").create();
		assertNotNull(f);
		
		id = (ObjectId)f.get("_id");
	}
	
	@Test
	public void existed(){
		Entity e = factory.get("user");
		assertNotNull(e);

		Expression expr = e.expr().equals("_id", id);
		assertTrue(e.filter(expr).exists("name", "value"));
		assertTrue(e.filter(expr).exists("_id"));
		assertTrue(e.filter(expr).exists("name"));
		assertTrue(!e.filter(expr).exists("value-dddd"));
	}
	
	@Test
	public void notExisted(){
		Entity e = factory.get("user");
		assertNotNull(e);

		Expression expr = e.expr().equals("_id", id);
		assertTrue(e.filter(expr).exists());
		assertTrue(!e.filter(expr).exists("gender"));
		assertTrue(!e.filter(expr).exists("don-t-existed"));

	}
}
