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

import com.mongodb.WriteConcern;
import org.canedata.entity.Entity;
import org.canedata.provider.mongodb.entity.Options;
import org.junit.Before;
import org.junit.Test;

/**
 * 
 * @author Sun Yat-ton
 * @version 1.00.000 2011-8-17
 */
public class TestDelete extends AbilityProvider {
	@Before
	public void setup() {
		initData();
	}

	@Test
	public void dById(){
		Entity e = factory.get("user");
		assertNotNull(e);
		
		long c = e.delete("id:test:1");
		assertEquals(c, 1);
	}
	
	@Test
	public void dByQuery(){
		Entity e = factory.get("user");
		assertNotNull(e);
		
		long c = e.deleteRange(e.expr().like("name", "cane"));
		assertEquals(c, 2);
	}

	@Test
	public void testOptions() {
		Entity e = factory.get("user");
		assertNotNull(e);

		long c = e.opt(Options.WRITE_CONCERN, WriteConcern.MAJORITY).deleteRange(e.expr().like("name", "cane"));
		// assertEquals(c, 2);
	}
}
