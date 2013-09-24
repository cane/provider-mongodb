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
import java.util.Date;

import org.bson.types.ObjectId;
import org.canedata.entity.Command;
import org.canedata.entity.Entity;
import org.canedata.field.Fields;
import org.canedata.provider.mongodb.command.Truncate;
import org.junit.BeforeClass;
import org.junit.Test;

import com.mongodb.WriteResult;

/**
 * 
 * @author Sun Yat-ton
 * @version 1.00.000 2011-8-7
 */
public class TestDataType extends AbilityProvider {
	static int i = 100;
	static int i2 = 1;
	static long l = 10000;
	static int c = 'B';
	static boolean b = true;
	static float f = 1.32f;
	static byte bt = 'd';
	static double d = 100.10;
	static byte[] bts = new byte[] { 'a', 'b' };
	static String s = "test";
	static short st = 1;
	static Date dt = new Date();

	static Fields fields = null;

	@BeforeClass
	public static void init() {
		Entity e = factory.get("user");

		Command truncate = new Truncate();
		WriteResult r = e.execute(truncate);
		System.out.println(r.getN());
		fields = e.put("int", i).put("int2", i2).put("long", l).put("char", c)
				.put("boolean", b).put("float", f).put("byte", bt)
				.put("double", d).put("bytes", bts).put("string", s)
				.put("short", st).put("date", dt).create();
	}

	@Test
	public void source() {
		assertNotNull(fields);

		assertEquals(fields.getInt("int"), i);
		assertEquals(fields.getInt("char"), c);
		assertEquals(fields.getLong("long"), l);
		assertEquals(fields.getBoolean("boolean"), b);
		assertTrue(Float.compare(fields.getFloat("float"), f) == 0);
		assertEquals(fields.getByte("byte"), bt);
		assertTrue(Arrays.equals(fields.getBytes("bytes"), bts));
		assertTrue(fields.getDouble("double") == d);
		assertEquals(fields.getString("string"), s);
		assertEquals(fields.getShort("short"), st);
		assertEquals(fields.getDate("date"), dt);
	}
	
	@Test
	public void restored(){
		assertNotNull(fields);
		
		Entity e = factory.get("user");
		Fields fs = e.restore((ObjectId)fields.get("_id"));

		assertEquals(fs.getInt("int"), i);
		assertEquals(fs.getChar("char"), c);
		assertEquals(fs.getLong("long"), l);
		assertEquals(fs.getBoolean("boolean"), b);
		assertTrue(Float.compare(fs.getFloat("float"), f) == 0);
		assertEquals(fs.getByte("byte"), bt);
		assertTrue(Arrays.equals(fs.getBytes("bytes"), bts));
		assertTrue(fs.getDouble("double") == d);
		assertEquals(fs.getString("string"), s);
		assertEquals(fs.getShort("short"), st);
		assertEquals(fs.getDate("date"), dt);
	}
	
	@Test
	public void interlace(){
		assertNotNull(fields);
		
		Entity e = factory.get("user");
		Fields fs = e.restore((ObjectId)fields.get("_id"));

		assertEquals(fs.getLong("int"), i);
		assertEquals(fs.getInt("char"), c);
		assertEquals(fs.getInt("long"), l);
		assertEquals(fs.getInt("boolean"), 1);
		assertTrue(fs.getBoolean("int2"));
		assertTrue(fs.getDouble("float") == f);
		assertEquals(fs.getChar("byte"), bt);
		assertTrue(Arrays.equals(fs.getBytes("bytes"), bts));
		assertTrue(Float.compare(fs.getFloat("double"), (float)d) == 0);
		assertEquals(fs.getString("string"), s);
		assertEquals(fs.getInt("short"), st);
		assertEquals(fs.getDate("date"), dt);
	}
}
