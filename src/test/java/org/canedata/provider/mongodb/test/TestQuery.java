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

import java.util.Arrays;
import java.util.List;

import org.canedata.entity.Entity;
import org.canedata.field.Fields;
import org.junit.Test;

/**
 * 
 * @author Sun Yat-ton
 * @version 1.00.000 2011-8-15
 */
public class TestQuery extends AbilityProvider {
	
	@Test
	public void proj(){
		Entity e = factory.get("user");
		assertNotNull(e);
		
		Fields f = e.projection("name", "age").orderDESC("age").first();
		assertNotNull(f);
		assertEquals(f.getFieldNames().length, 3);
		
		f = e.orderDESC("age").first();
		assertNotNull(f);
		assertEquals(Arrays.toString(f.getFieldNames()), "[_id, age, name, gender]");
		assertEquals(f.getFieldNames().length, 4);
		
	}
	
	@Test
	public void equals(){
		Entity e = factory.get("user");
		assertNotNull(e);
		
		Fields f = e.projection("name").filter(e.expr().equals("name", "cane")).first();
		assertNotNull(f);
		assertEquals(f.getString("name"), "cane");
		
		e.close();
	}
	
	@Test
	public void notEquals(){
		Entity e = factory.get("user");
		assertNotNull(e);
		
		int c = e.projection("name").filter(e.expr().notEquals("name", "cane")).count().intValue();
		assertEquals(c, 7);
		
		e.close();
	}
	
	@Test
	public void between(){
		Entity e = factory.get("user");
		assertNotNull(e);
		
		assertEquals(e.projection("name", "age").filter(e.expr().between("age", 18, 64)).count(), 3l);
		
		e.close();
	}
	
	@Test
	public void notBetween(){
		Entity e = factory.get("user");
		assertNotNull(e);
		
		assertEquals(e.projection("name", "age").filter(e.expr().notBetween("age", 63, 64)).count(), 6l);
		
		e.close();
	}
	
	@Test
	public void greaterThan(){
		Entity e = factory.get("user");
		assertNotNull(e);
		
		assertEquals(e.filter(e.expr().greaterThan("age", 18)).count(), 2l);
		
		e.close();
	}
	
	@Test
	public void greaterThanOrEqual(){
		Entity e = factory.get("user");
		assertNotNull(e);
		
		Fields f = e.filter(e.expr().greaterThanOrEqual("age", 19)).order("age").first();
		assertNotNull(f);
		
		assertEquals(f.getInt("age"), 19);
		
		e.close();
	}
	
	@Test
	public void in(){
		Entity e = factory.get("user");
		assertNotNull(e);
		
		List<Fields> fs = e.filter(e.expr().in("age", new Object[]{18, 19})).order("age").list();
		assertNotNull(fs);
		assertTrue(!fs.isEmpty());
		
		assertEquals(fs.size(), 2);
		
		Fields f1 = fs.get(0);
		assertEquals(f1.getInt("age"), 18);
		
		Fields f2 = fs.get(1);
		assertEquals(f2.getInt("age"), 19);
		
		e.close();
	}
	
	@Test
	public void notIn(){
		Entity e = factory.get("user");
		assertNotNull(e);
		
		int c = e.filter(e.expr().notIn("age", new Object[]{18, 19})).order("age").count().intValue();
		assertEquals(c, 6);
		
		e.close();
	}
	
	@Test
	public void empty(){
		Entity e = factory.get("user");
		assertNotNull(e);
		
		int c = e.filter(e.expr().isEmpty("vendor")).count().intValue();
		assertEquals(c, 1);
		
		e.close();
	}
	
	@Test
	public void notEmpty(){
		Entity e = factory.get("user");
		assertNotNull(e);
		
		int c = e.filter(e.expr().isNotEmpty("vendor")).count().intValue();
		assertEquals(c, 1);
		
		e.close();
	}
	
	@Test
	public void isNull(){
		Entity e = factory.get("user");
		assertNotNull(e);
		
		int c = e.filter(e.expr().isNull("name")).count().intValue();
		assertEquals(c, 4);
		
		
		e.close();
	}
	
	@Test
	public void notNull(){
		Entity e = factory.get("user");
		assertNotNull(e);
		
		int c = e.filter(e.expr().isNotNull("name")).count().intValue();
		assertEquals(c, 4);
		
		e.close();
	}
	
	@Test
	public void lessthan(){
		Entity e = factory.get("user");
		assertNotNull(e);
		
		int c = e.filter(e.expr().lessThan("age", 18)).count().intValue();
		assertEquals(c, 4);
		
		e.close();
	}
	
	@Test
	public void lessthanAndEq(){
		Entity e = factory.get("user");
		assertNotNull(e);
		
		int c = e.filter(e.expr().lessThanOrEqual("age", 18)).count().intValue();
		assertEquals(c, 5);
		
		e.close();
	}
	
	@Test
	public void like(){
		Entity e = factory.get("user");
		assertNotNull(e);
		
		int c = e.filter(e.expr().like("name", "cane")).count().intValue();
		assertEquals(c, 2);
	}
	
	@Test
	public void notLike(){
		Entity e = factory.get("user");
		assertNotNull(e);
		
		int c = e.filter(e.expr().notLike("name", "cane")).count().intValue();
		assertEquals(c, 6);
	}
	
	@Test
	public void match(){
		Entity e = factory.get("user");
		assertNotNull(e);
		
		int c = e.filter(e.expr().match("name", "cane.*")).count().intValue();
		assertEquals(c, 2);
		
		e.close();
	}
	
	@Test
	public void notMatch(){
		Entity e = factory.get("user");
		assertNotNull(e);
		
		int c = e.filter(e.expr().notMatch("name", "cane.*")).count().intValue();
		assertEquals(c, 6);
		
		e.close();
	}
}
