/**
 * Copyright 2012 CaneData.org
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
package org.canedata.provider.mongodb.test.expr;

import com.mongodb.*;
import com.mongodb.util.JSON;
import org.canedata.entity.Command;
import org.canedata.entity.Entity;
import org.canedata.entity.EntityFactory;
import org.canedata.field.Fields;
import org.canedata.provider.mongodb.entity.MongoEntity;
import org.canedata.provider.mongodb.test.AbilityProvider;
import org.canedata.resource.Resource;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

/**
 * 
 * @author Sun Yat-ton
 * @version 1.00.000 2012-2-25
 */
public class TestSlice extends AbilityProvider {
	@Test
	public void slice(){
		MongoEntity e = (MongoEntity)factory.get("user");
		assertNotNull(e);

		BasicDBObject proj = new BasicDBObject();
		proj.append("sub", new BasicDBObject().append("$slice", new Integer[]{1, 2}));


		Fields rlt = e.projection(proj).filter(e.expr().equals("name", "multi")).first();
		assertNotNull(rlt);
		assertNotNull(rlt.get("sub"));
		assertEquals(((List) rlt.get("sub")).size(), 2);
	}

	@Test
	public void cm(){
		Entity e = factory.get("user");
		assertNotNull(e);

		e.execute(new Command() {
			public String getName() {
				return "testSlice";
			}

			public String describe() {
				return "test slice";
			}

			public <D> D execute(EntityFactory factory, Resource<?> res, Entity target, Object... args) {
				Resource<DB> dbr = (Resource<DB>)res;
				DB db = dbr.take();

				DBCollection collection = db.getCollection("user");

				BasicDBObject query = new BasicDBObject();
				query.put("_id","multixxx");

				DBCursor resultsCursor = collection.find(query, new BasicDBObject().append("sub", new BasicDBObject().append("$slice", new int[]{2, 2})));
				while (resultsCursor.hasNext()) {
					DBObject r = resultsCursor.next();
					System.out.println(JSON.serialize(r));
				}
				return null;
			}
		});
	}

	
}
