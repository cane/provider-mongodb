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
package org.canedata.provider.mongodb;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.canedata.core.logging.LoggerFactory;
import org.canedata.core.util.StringUtils;
import org.canedata.logging.Logger;
import org.canedata.resource.Resource;
import org.canedata.resource.meta.EntityMeta;
import org.canedata.resource.meta.RepositoryMeta;

import com.mongodb.DB;
import com.mongodb.Mongo;

/**
 * 
 * @author Sun Yat-ton
 * @version 1.00.000 2011-8-1
 */
public abstract class MongoResource implements Resource<DB> {
	final static Logger logger = LoggerFactory.getLogger(MongoResource.class);

	private static String NAME = "Resource for mongo-java-driver";
	private static String VERSION = "2.6.3";

	abstract Mongo getMongo();

	abstract MongoResourceProvider getProvider();

	public boolean isWrappedFor(Class<?> iface) {
		return iface.isInstance(this);
	}

	public <T> T unwrap(Class<T> iface) {
		return iface.cast(getMongo());
	}

	public String getName() {
		return NAME;
	}

	public String getVersion() {
		return VERSION;
	}

	public List<RepositoryMeta> getRepositories() {
		List<String> dbs = getMongo().getDatabaseNames();
		List<RepositoryMeta> reps = new ArrayList<RepositoryMeta>(dbs.size());

		for (final String name : dbs) {
			reps.add(new RepositoryMeta() {
				String label = name;

				public String getLabel() {
					return label;
				}

				public Object getAttribute(String name) {
					// TODO Auto-generated method stub
					return null;
				}

				public Map<String, Object> getAttributes() {
					// TODO Auto-generated method stub
					return null;
				}

				public int sequence() {
					// TODO Auto-generated method stub
					return 0;
				}

				public String describe() {
					// TODO Auto-generated method stub
					return null;
				}

				public List<EntityMeta> getEntities() {
					// TODO Auto-generated method stub
					return null;
				}

				public EntityMeta getEntity(String name) {
					// TODO Auto-generated method stub
					return null;
				}

				public String getName() {
					return name;
				}

			});
		}

		return reps;
	}

	public DB take() {
		String dbname = getProvider().getDefaultDBName();
		if (StringUtils.isBlank(dbname)) {
			dbname = getMongo().getDatabaseNames().get(0);
		}

		return take(dbname);
	}

	public DB take(Object... args) {
		if (args == null || args.length == 0)
			throw new IllegalArgumentException(
					"Must specify the database name.");

		String dbname = (String) args[0];
		logger.debug("Taking the resource {0}.", dbname);

		DB db = getMongo().getDB(dbname);

		if (getProvider().getAuthentications().containsKey(dbname)) {
			String[] auth = getProvider().getAuthentications().get(dbname);

			if (!db.authenticate(auth[0], auth[1].toCharArray())) {
				throw new RuntimeException("Authenticate failure!");
			}
		}

		db.requestStart();
		db.requestEnsureConnection();
		return db;
	}

	public DB take(boolean exclusive) {
		return take();
	}

	public DB take(boolean exclusive, Object... args) {
		return take(args);
	}

	public DB checkout() {
		return take(true);
	}

	public DB checkout(Object... args) {
		return take(true, args);
	}

	public Resource<DB> release(Object target) {
		if (!(target instanceof DB))
			throw new IllegalArgumentException(
					"Specifies the instance of the target is not "
							+ DB.class.getName());

		logger.debug("Release the resource {0}.", ((DB) target).getName());

		((DB) target).requestDone();

		return this;
	}

}
