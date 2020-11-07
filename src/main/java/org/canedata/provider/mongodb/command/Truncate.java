/**
 * Copyright 2013 CaneData.org
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.canedata.provider.mongodb.command;

import com.mongodb.client.ListIndexesIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.result.DeleteResult;
import org.bson.BSONObject;
import org.bson.Document;
import org.canedata.core.logging.LoggerFactory;
import org.canedata.entity.Command;
import org.canedata.entity.Entity;
import org.canedata.entity.EntityFactory;
import org.canedata.logging.Logger;
import org.canedata.provider.mongodb.MongoResource;
import org.canedata.resource.Resource;

import com.mongodb.BasicDBObject;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedSet;

/**
 * This Truncate command called the Remove method of Collection, when a large
 * quantity of data, do less efficient.
 *
 * @author Sun Yat-ton
 * @version 1.00.000 2012-2-25
 */
final public class Truncate implements Command {
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
        MongoResource mres = (MongoResource) res;

        MongoDatabase db = null;
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

        MongoCollection coll = db.getCollection(coll_name);
        ListIndexesIterable<Document> stored_index = coll.listIndexes();

        List<Document> idx = new ArrayList<>();
        stored_index.forEach(i -> {
            idx.add(i);
        });

        coll.drop();

        idx.forEach(i -> {
            IndexOptions io = new IndexOptions();
            if(i.containsKey("name"))
                io.name(i.getString("name"));

            if(i.containsKey("unique"))
                io.unique(i.getBoolean("unique"));

            coll.createIndex(i.get("key", Document.class), io);
        });

        return null;
    }
}
