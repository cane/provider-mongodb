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

import org.canedata.entity.Entity;
import org.junit.Before;
import org.junit.Test;

/**
 * 
 * @author Sun Yat-ton
 * @version 1.00.000 2011-8-15
 */
public class TestCount extends AbilityProvider{
	private static final String ENTITY = "user";

	@Before
	public void setup() {
		initData();
	}

	@Test
	public void sample(){
		Entity e = factory.get(ENTITY);
		assertNotNull(e);

		int c = e.count().intValue();
		assertEquals(c, 9);
	}
	
	@Test
	public void like(){
		Entity e = factory.get(ENTITY);
		assertNotNull(e);
		
		int c = e.filter(e.expr().like("name", "cane")).count().intValue();
		assertEquals(c, 2);
	}
	
	@Test
	public void notLike(){
		Entity e = factory.get(ENTITY);
		assertNotNull(e);
		
		int c = e.filter(e.expr().notLike("name", "cane")).count().intValue();
		assertEquals(c, 7);
	}
	
	@Test
	public void retain(){
		Entity e = factory.get(ENTITY);
		assertNotNull(e);
		
		int c = e.filter(e.expr().like("name", "cane")).count().intValue();
		assertEquals(c, 2);
		
		c = e.filter(e.expr().notLike("name", "cane")).count().intValue();
		assertEquals(c, 7);
	}
}
