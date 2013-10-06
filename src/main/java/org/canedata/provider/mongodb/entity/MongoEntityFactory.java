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

import com.mongodb.DB;
import com.mongodb.DBCollection;

/**
 * 
 * @author Sun Yat-ton
 * @version 1.00.000 2011-8-1
 */
public abstract class MongoEntityFactory implements EntityFactory {
	protected static final Logger logger = LoggerFactory
			.getLogger(MongoEntityFactory.class);

	abstract protected ResourceProvider getResourceProvider();

	public Entity get(String name) {
		return get(null, null, name);
	}

	public Entity get(String schema, String name) {
		return get(null, schema, name);
	}

	public Entity get(Resource<?> res, String name) {
		return get(res, null, name);
	}

	public Entity get(final Resource<?> res, final String schema,
			final String name) {
		MongoResource mres = (MongoResource) res;
		if (null == mres) {
			mres = (MongoResource) getResourceProvider().getResource();
		}

		final MongoResource cres = mres;

		return new MongoEntity() {
			ThreadLocal<MongoIntent> lIntent = new ThreadLocal<MongoIntent>();
            ThreadLocal<DB> lDb = new ThreadLocal<DB>();
            DBCollection collection = null;

			String cacheSchema = null;
			String label = name;

			public String getSchema() {
				if (StringUtils.isBlank(schema))
					return getCollection().getDB().getName();

				return schema;
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
				MongoIntent intent = lIntent.get();
				if (null == intent) {
					intent = new MongoIntent(this);
					lIntent.set(intent);
				}

				return intent;
			}

			public void close() {
				if (hasClosed)
					return;

				hasClosed = true;
                DB db = lDb.get();

				if (null != db)
					cres.release(db);
			}

			@Override
			MongoResource getResource() {
				return cres;
			}

			@Override
			DBCollection getCollection() {
                if(null != collection)
                    return collection;

                DB db = lDb.get();

				if (null == db){
					db = cres.take();
                    lDb.set(db);
                }

                collection = db.getCollection(name);
				return collection;
			}

			@Override
			MongoEntityFactory getFactory() {
				return MongoEntityFactory.this;
			}

			private String getCacheSchema() {
				if (StringUtils.isBlank(cacheSchema))
					cacheSchema = MongoEntityFactory.this.getName().concat(":")
							.concat(getSchema()).concat(":").concat(getName());

				return cacheSchema;
			}

			@Override
			Cache getCache() {
				if (null == MongoEntityFactory.this.getCacheProvider())
					return null;

				return MongoEntityFactory.this.getCacheProvider().getCache(
						getCacheSchema());
			}

		};
		
	}

}
