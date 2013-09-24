/**
 * Copyright 2013 CaneData.org
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
package org.canedata.provider.mongodb.command;

import org.canedata.core.logging.LoggerFactory;
import org.canedata.entity.Command;
import org.canedata.entity.Entity;
import org.canedata.entity.EntityFactory;
import org.canedata.logging.Logger;
import org.canedata.provider.mongodb.MongoResource;
import org.canedata.resource.Resource;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.WriteResult;

/**
 * This Truncate command called the Remove method of Collection, when a large
 * quantity of data, do less efficient.
 * 
 * @author Sun Yat-ton
 * @version 1.00.000 2012-2-25
 */
public class Truncate implements Command {
	static final Logger logger = LoggerFactory.getLogger(Truncate.class);

	private static final String NAME = "truncate";


	public String getName() {
		return NAME;
	}

	public String describe() {
		return "Empties a collection completely.";
	}

	public <D> D execute(EntityFactory factory, Resource<?> res,
			Entity target, Object... args) {
		// specified collection name.
		// or use default collection.
		MongoResource mres = (MongoResource)res;
		
		DB db = null;
		String coll_name = target.getName();

		if (args != null) {
			// take db from resource.
			if (args.length >= 2) {
				db = mres.take(args[0]);
				coll_name = (String) args[1];
			} else {
				db = mres.take();

				if (args.length >= 1)
					coll_name = (String) args[0];
			}

		}

		DBCollection coll = db.getCollection(coll_name);

		WriteResult rlt = coll.remove(new BasicDBObject());
		return (D) rlt;
	}
}
