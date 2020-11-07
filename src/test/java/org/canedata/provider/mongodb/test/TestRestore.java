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
import static org.junit.Assert.assertTrue;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Objects;
import java.util.Optional;

import org.bson.types.ObjectId;
import org.canedata.entity.Entity;
import org.canedata.exception.EntityNotFoundException;
import org.canedata.field.Fields;
import org.junit.Before;
import org.junit.Test;

/**
 * 
 * @author Sun Yat-ton
 * @version 1.00.000 2011-8-17
 */
public class TestRestore extends AbilityProvider {
	@Before
	public void setup() {
		initData();
	}

	@Test
	public void strId(){
		Entity e = factory.get("user");
		assertNotNull(e);
		
		Fields f = e.restore("id:test:1");
		assertNotNull(f);
	}
	
	
	@Test
	public void allFields() {
		Entity e = factory.get("user");
		assertNotNull(e);

		Fields f = e.put("name", "cane").put("vendor", "cane team").put("desc", "test all fields").create();
		assertNotNull(f);
		assertNotNull(f.get("_id"));

		ObjectId o = f.get("_id");
		Fields rlt = e.restore(o);

		assertTrue(o.compareTo(rlt.get("_id")) == 0);
		assertEquals(rlt.getString("name"), "cane");
		assertEquals(rlt.getString("vendor"), "cane team");
		
		e.close();
	}

	@Test
	public void proj() {
		Entity e = factory.get("user");
		assertNotNull(e);

		Fields f = e.put("name", "cane").put("vendor", "cane team").create();
		assertNotNull(f);
		assertNotNull(f.get("_id"));

		ObjectId id = f.get("_id");
		Fields rlt = e.projection("name").restore(id);

		assertEquals(f.get("_id").toString(), rlt.get("_id").toString());
		assertEquals(rlt.getString("name"), "cane");
		assertNull(rlt.getString("vendor"));
		
		
		f = e.restore("id:test:1");
		assertEquals(f.getFieldNames().length, 4);
		
		Fields f2 = e.projection("age").restore("id:test:1");
		assertEquals(f2.getFieldNames().length, 2);
		
		
		e.close();
	}
	
	@Test
	public void date(){
		Entity e = factory.get("user");
		assertNotNull(e);
		
		Date d = new Date();
		
		Fields r = e.put("date", d).create();
		ObjectId id = r.get("_id");
		Fields f = e.restore(id);
		assertNotNull(f);
		
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm");
		assertTrue(d.equals(f.getDate("date")));
	}
	
	@Test
	public void notExist(){
		Entity e = factory.get("user");
		assertNotNull(e);
		
		try{
			e.relate("--xxxdd--bbccee--jk//--");
		}catch(EntityNotFoundException enfe){
			assertNotNull(enfe);
		}
		
		e.close();
	}
}
