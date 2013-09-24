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
import org.canedata.field.Fields;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * 
 * @author Sun Yat-ton
 * @version 1.00.000 2011-8-9
 */
public class TestExist extends AbilityProvider {
	static ObjectId id;
	@BeforeClass
	public static void init(){
		Entity e = factory.get("user");
		assertNotNull(e);
	
		Fields f = e.put("name", "cane").put("value", "v").create();
		assertNotNull(f);
		
		id = f.get("_id");
		
		e.close();
	}
	
	@Test
	public void existed(){
		Entity e = factory.get("user");
		assertNotNull(e);
		
		assertTrue(e.exists(id, "name", "value"));
		assertTrue(e.exists(id));
		assertTrue(e.exists(id, "name"));
		assertTrue(e.exists(id, "value"));
		
		e.close();
	}
	
	@Test
	public void notExisted(){
		Entity e = factory.get("user");
		assertNotNull(e);
		
		assertTrue(e.exists(id));
		assertTrue(!e.exists(id, "gender"));
		assertTrue(!e.exists(new ObjectId()));
		
		e.close();
	}
}
