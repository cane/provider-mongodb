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
package org.canedata.provider.mongodb.entity;

import com.mongodb.BasicDBObject;
import com.mongodb.ReadConcern;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.canedata.cache.Cache;
import org.canedata.core.logging.LoggerFactory;
import org.canedata.core.util.StringUtils;
import org.canedata.entity.Entity;
import org.canedata.entity.EntityFactory;
import org.canedata.logging.Logger;
import org.canedata.provider.mongodb.MongoResource;
import org.canedata.provider.mongodb.intent.MongoIntent;
import org.canedata.resource.Resource;
import org.canedata.resource.ResourceProvider;

import java.util.HashMap;
import java.util.Map;

/**
 * 
 * @author Sun Yat-ton
 * @version 1.00.000 2011-8-1
 */
public abstract class MongoEntityFactory implements EntityFactory<MongoEntity> {
	protected static final Logger logger = LoggerFactory
			.getLogger(MongoEntityFactory.class);

	abstract protected ResourceProvider getResourceProvider();
    abstract protected String getDefaultSchema();

	public MongoEntity get(String name) {
		return get(null, null, name);
	}

	public MongoEntity get(String schema, String name) {
		return get(null, schema, name);
	}

	public MongoEntity get(Resource<?> res, String name) {
		return get(res, null, name);
	}

	public MongoEntity get(final Resource<?> res, final String schema,
			final String name) {
		MongoResource mres = (MongoResource) res;
		if (null == mres) {
			mres = (MongoResource) getResourceProvider().getResource();
		}

		final MongoResource cres = mres;
        final String final_schema = StringUtils.isBlank(schema)?getDefaultSchema():schema;

		return new MongoEntity() {
            ThreadLocal<Map<String, MongoDatabase>> lDb = new ThreadLocal<>();

            MongoCollection collection = null;

			String label = name;
            MongoIntent intent = new MongoIntent(this);

			public String getSchema() {
				return final_schema;
			}

			public String getName() {
				return name;
			}

			public String getLabel() {
				return label;
			}

			public Entity label(String label) {
				this.label = label;
				
				return this;
			}

			public String label() {
				return label;
			}

			public Entity alias(String label) {
				return label(label);
			}

			/**
			 * thread safe
			 */
			@Override
			protected MongoIntent getIntent() {
				return intent;
			}

			public void close() {
				if (hasClosed)
					return;

				hasClosed = true;
				MongoDatabase db = getDb(final_schema);

				if (null != db)
					cres.release(db);

                Map<String, MongoDatabase> dbs = lDb.get();
                dbs.remove(final_schema);
			}

			@Override
			MongoResource getResource() {
				return cres;
			}

			@Override
			MongoCollection getCollection() {
                if(null != collection)
                    return collection;

				MongoDatabase db = getDb(final_schema);

                collection = db.getCollection(name);
				return collection;
			}

            private MongoDatabase getDb(String schema){
                Map<String, MongoDatabase> dbs = lDb.get();
                if(null == dbs){
                    dbs = new HashMap<String, MongoDatabase>();
                    lDb.set(dbs);
                }

				MongoDatabase db = dbs.get(schema);
                if(null == db){
                    db = cres.take(schema);

                    dbs.put(schema, db);
                }

                return db;
            }

			@Override
			MongoEntityFactory getFactory() {
				return MongoEntityFactory.this;
			}

			/**
			 * The caching strategy can be specified for each collection,
			 * Or use the default strategy uniformly, if not specified.
			 */
			private String getCacheSchema() {
					return MongoEntityFactory.this.getName().concat(":")
							.concat(getSchema()).concat(":").concat(getName());
			}

			Cache cache = null;
			@Override
			Cache getCache() {
				if(null != cache) return cache;

				if (null == MongoEntityFactory.this.getCacheProvider())
					return null;

				cache = MongoEntityFactory.this.getCacheProvider().getCache(
						getCacheSchema());

				return cache;
			}

		};
		
	}

}
