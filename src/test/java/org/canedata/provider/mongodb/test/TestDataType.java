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

import com.mongodb.BasicDBObject;
import com.mongodb.client.result.DeleteResult;
import org.bson.Document;
import org.bson.types.Binary;
import org.bson.types.Code;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;
import org.canedata.entity.Command;
import org.canedata.entity.Entity;
import org.canedata.field.Fields;
import org.canedata.provider.mongodb.command.Truncate;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.*;

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
	static String[] ss = new String[] { "a", "b"};
	static short st = 1;
	static Date dt = new Date();
	static BigInteger bit = BigInteger.valueOf(123l);
	static BigDecimal bdc = BigDecimal.valueOf(1.2);
	static Number number = 1l;
	static List<String> list = Arrays.asList("a", "b", "c");
	static Map<String, Object> map = new HashMap<String, Object>(){{
		put("a","b");
		put("b","b");
	}};

	static Document doc = new Document(){{
		put("a", "a");
	}};
	static BasicDBObject bdbo = new BasicDBObject(){{
		put("aa", "aa");
	}};

	static Fields fields = null;

    @Before
	public void init() {
    	//clear();

		Entity e = factory.get("user");

		Command truncate = new Truncate();
		DeleteResult r = e.execute(truncate);

		fields = e.put("int", i).put("int2", i2).put("long", l).put("char", c)
				.put("boolean", b).put("float", f).put("byte", bt)
				.put("double", d).put("bytes", bts).put("string", s)
				.put("short", st).put("date", dt).put("list", list).put("map", map)
				.put("doc", doc).put("bdbo", bdbo)
				.put("number", number)
				.put("bit", bit)
				.put("bdc", bdc)
				.put("ss", ss)
				.put("atoml", new AtomicLong(12))
				.put("code", new Code("Strings(['a','b'])"))
				.create();
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
		assertEquals(fields.get("list"), list);
		assertEquals(fields.get("map"), map);
		assertEquals(fields.get("doc"), doc);
		assertEquals(fields.get("bdbo"), bdbo);
		assertEquals(fields.get("number"), number);
		//assertEquals(fields.get("number"), 1l);
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
		assertEquals(fs.get("list"), list);
		Map<String, Object> map = (Map<String, Object>) fs.get("map");
		assertTrue(fs.get("map").equals(map));
		assertEquals(((Document)fs.get("doc")).toJson(), doc.toJson());
		//BasicDBObject _bdbo = (BasicDBObject)fs.get("bdbo"); throw exception
		Document _d_bdbo = (Document)fs.get("bdbo");
		BasicDBObject _bdbo = new BasicDBObject();
		_bdbo.putAll(_d_bdbo);
		assertEquals(bdbo.toJson(), ((Document)fs.get("bdbo")).toJson());
		assertEquals(bdbo, _bdbo);

		assertEquals(fs.get("number"), number);
		Decimal128 _bd = fs.get("bdc");
		assertTrue(_bd.bigDecimalValue().compareTo(bdc) == 0);

		//BigInteger _bit = fs.get("bit");
		//assertEquals(bit.intValue(), _bit.intValue());
		assertNotNull(fs.get("ss"));
		//assertEquals(ss, (String[])fs.get("ss"));
	}

	@Test
	public void findByBigInteger(){
		Entity e = factory.get("user");
		Fields fs = e.findOne(e.filter().equals("bit", bit));
		assertNotNull(fs);
		//System.out.println(fs.getWrapped(Document.class).toJson());
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
