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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Date;

import org.bson.Document;
import org.canedata.entity.Entity;
import org.canedata.field.Fields;
import org.junit.Before;
import org.junit.Test;

/**
 * 
 * @author Sun Yat-ton
 * @version 1.00.000 2011-8-5
 */
public class TestCreate extends AbilityProvider {
	@Before
	public void setup(){
		clear();
	}
	@Test
	public void autoGenId(){
		Entity e = factory.get("user");
		assertNotNull(e);
		
		Fields f = e.put("name", "cane").put("vendor", "cane team").create();
		assertNotNull(f);

		assertNotNull(f.get("_id"));
		
		e.close();
	}
	
	@Test
	public void staticId(){
		Entity e = factory.get("user");
		assertNotNull(e);
		
		String key = "key:test:" + new java.util.Random().nextInt();
		Fields f = e.put("key", key).put("content", "ddddddd".getBytes()).create(key);
		assertNotNull(f);
		assertEquals(f.getString("_id"), key);
		
		
	}
	
	@Test
	public void testDate(){
		Entity e = factory.get("user");
		assertNotNull(e);
		
		Date date = new Date();
		Fields f = e.put("name", "cane").put("date", date).create();
		assertNotNull(f);
		
		assertTrue(date.equals(f.getDate("date")));
	}
}
