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

import org.canedata.entity.Entity;
import org.junit.Before;
import org.junit.Test;

/**
 * 
 * @author Sun Yat-ton
 * @version 1.00.000 2011-8-17
 */
public class TestRevive extends AbilityProvider {
	@Before
	public void setup() {
		initData();
	}

	@Test
	public void closed(){
		Entity e = factory.get("user");
		assertNotNull(e);
		
		assertEquals(e.count(), 9l);
		e.close();
		
		try{
			e.count();
		}catch(RuntimeException re){
			assertNotNull(re);
		}
	}
	
	@Test
	public void revive(){
		Entity e = factory.get("user");
		assertNotNull(e);
		
		assertEquals(e.count(), 9l);
		e.close();
		
		try{
			e.revive();
			assertEquals(e.count(), 9l);
		}catch(RuntimeException re){
			assertNull(re);
		}
	}
}
