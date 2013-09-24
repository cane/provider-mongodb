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
import static org.junit.Assert.assertNull;

import java.util.Date;

import org.canedata.entity.Entity;
import org.canedata.field.Fields;
import org.junit.Test;

import com.mongodb.BasicDBObject;

/**
 * 
 * @author Sun Yat-ton
 * @version 1.00.000 2011-8-17
 */
public class TestUpdate extends AbilityProvider{
	@Test
	public void uById(){
		Entity e = factory.get("user");
		assertNotNull(e);
		
		int c = e.put("$inc", new BasicDBObject().append("age", 19)).put("4up", "4up").put("upd", 12).update("id:test:1");
		assertEquals(c, 1);
		
		Fields f = e.restore("id:test:1");
		assertEquals(f.getInt("upd"), 12);
		assertEquals(f.getInt("age"), 32);
		assertEquals(f.getString("4up"), "4up");
	}
	
	@Test
	public void uByFilter(){
		Entity e = factory.get("user");
		assertNotNull(e);
		
		int c = e.put("upd2", new Date()).put("$inc", new BasicDBObject().append("4inc", 1)).updateRange(e.expr().like("name", "cane"));
		assertEquals(c, 2);
		
	}
	
	@Test
	public void unset(){
		Entity e = factory.get("user");
		assertNotNull(e);
		
		int c = e.put("$unset", new BasicDBObject().append("4up", null)).update("id:test:1");
		assertEquals(c, 1);
		
		Fields f = e.restore("id:test:1");
		assertNull(f.get("4up"));
	}
}
