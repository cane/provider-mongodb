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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.mongodb.ClientSessionOptions;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import org.bson.codecs.Codec;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;
import org.canedata.core.logging.LoggerFactory;
import org.canedata.core.util.StringUtils;
import org.canedata.logging.Logger;
import org.canedata.provider.mongodb.codecs.BigIntegerCodec;
import org.canedata.resource.Resource;
import org.canedata.resource.meta.EntityMeta;
import org.canedata.resource.meta.RepositoryMeta;

/**
 * update to driver 4.1
 * @author Sun Yitao
 * @version 1.1.0 2020-08-24
 *
 * @author Sun Yat-ton
 * @version 1.00.000 2011-8-1
 */
public abstract class MongoResource implements Resource<MongoDatabase> {
	final static Logger logger = LoggerFactory.getLogger(MongoResource.class);

	private static String NAME = "Resource for mongo-java-driver";
	private static String VERSION = "2.6.3";

	abstract MongoClient getMongo();

	abstract MongoResourceProvider getProvider();

	public boolean isWrappedFor(Class<?> iface) {
		return iface.isInstance(getMongo());
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
		List<String> dbs = new ArrayList<>();
		/*getMongo().listDatabaseNames().forEach((i) -> {
			System.out.println(i);
		});*/
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

	public MongoDatabase take() {
		String dbname = getProvider().getDefaultDBName();
		if (StringUtils.isBlank(dbname)) {
			dbname = getRepositories().get(0).getName();
		}

		return take(dbname);
	}

	public MongoDatabase take(Object... args) {
		if (args == null || args.length == 0)
			throw new IllegalArgumentException(
					"Must specify the database name.");

		String dbname = (String) args[0];
		if(logger.isDebug())
            logger.debug("Taking the resource {0}.", dbname);

		MongoDatabase db = getMongo().getDatabase(dbname);

		if (getProvider().getAuthentications().containsKey(dbname)) {
			String[] auth = getProvider().getAuthentications().get(dbname);

			/* TODO implement for driver 4.1
			if (!db.authenticate(auth[0], auth[1].toCharArray())) {
				throw new RuntimeException("Authenticate failure!");
			}*/
		}

		return prepareCodec(db);
	}

	private MongoDatabase prepareCodec(MongoDatabase origin) {
		if (getProvider().getCodecRegistry() != null)
			return origin.withCodecRegistry(getProvider().getCodecRegistry());

		List<Codec<?>> _codecs = getProvider().getCodecs();
		if(!_codecs.isEmpty()) {
			CodecRegistry _codec = CodecRegistries.fromRegistries(origin.getCodecRegistry(), CodecRegistries.fromCodecs(_codecs));
			return origin.withCodecRegistry(_codec);
		}

		return origin;
	}

	public MongoDatabase take(boolean exclusive) {
		return take();
	}

	public MongoDatabase take(boolean exclusive, Object... args) {
		return take(args);
	}

	public MongoDatabase checkout() {
		return take(true);
	}

	public MongoDatabase checkout(Object... args) {
		return take(true, args);
	}

	public Resource<MongoDatabase> release(Object target) {
		if (!(target instanceof MongoDatabase))
			throw new IllegalArgumentException(
					"Specifies the instance of the target is not "
							+ MongoDatabase.class.getName());

		if(logger.isDebug())
            logger.debug("Release the resource {0}.", ((MongoDatabase) target).getName());

		//((MongoDatabase) target).drop();

		return this;
	}

	@Override
	public void close() {
		//throw new RuntimeException("Don't implemented!!!");
	}

	@Override
	public boolean hasClosed() {
		return false;
	}
}
