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

import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.canedata.entity.Entity;
import org.canedata.expression.Expression;
import org.canedata.field.Field;
import org.canedata.field.Fields;
import org.canedata.provider.mongodb.entity.MongoEntity;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

/**
 * 
 * @author Sun Yat-ton
 * @version 1.00.000 2011-8-9
 */
public class TestAggregate extends AbilityProvider {
	@Before
	public void setup() {
		initData();
	}

	@Test
	public void groupSum(){
		MongoEntity e = factory.get("user");
		assertNotNull(e);

		/**
		 * .runCommand( {
		 *    aggregate: "articles",
		 *    pipeline: [
		 *       { $project: { tags: 1 } },
		 *       { $unwind: "$tags" },
		 *       { $group: { _id: "$tags", count: { $sum : 1 } } }
		 *    ],
		 *    cursor: { }
		 * } )
		 *
		 * db.orders.aggregate( [
		 *       { $match: { status: "A" } },
		 *       { $group: { _id: "$cust_id", total: { $sum: "$amount" } } },
		 *       { $sort: { total: -1 } },
		 *       { $limit: 2 }
		 *    ],
		 *    { cursor: { batchSize: 0 } }
		 * )
		 */
		List<Fields> rlt = e.aggregate(new ArrayList<Bson>(){{
			add(new Document().append("$match", new Document().append("_id", new Document().append("$exists", 1))));
			add(new Document().append("$group", new Document().append("_id", "$age").append("count", new Document().append("$sum", 1))));
		}});

		rlt.stream().filter(i -> { return i.getInt("age") == 13; }).forEach(i -> { assertEquals(i.getInt("count"), 3);});
		/*rlt.stream().forEach(i -> {
			System.out.println(i.getWrapped().toString());
		});*/
	}
	

}
