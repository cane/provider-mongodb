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
package org.canedata.provider.mongodb.pool;

import java.util.NoSuchElementException;

import org.apache.commons.pool.KeyedObjectPool;
import org.apache.commons.pool.impl.StackKeyedObjectPool;
import org.canedata.core.logging.LoggerFactory;
import org.canedata.logging.Logger;
import org.canedata.provider.mongodb.field.MongoFields;

/**
 * 
 * @author Sun Yat-ton
 * @version 1.00.000 2011-9-2
 */
public class FieldsPool {
	protected static final Logger logger = LoggerFactory
			.getLogger(FieldsPool.class);

	private static FieldsPool instance = null;
	private PoolableFactory factory = new PoolableFactory();
	private KeyedObjectPool pool = null;
	private int max = 2000;
	private static String KEY = "mongofields";

	private FieldsPool() {
		pool = new StackKeyedObjectPool(factory, max, 10);
	}

	public static FieldsPool getInstance() {
		if (null == instance)
			instance = new FieldsPool();

		return instance;
	}

	public MongoFields borrow() {
		if(logger.isDebug())
            logger.debug("Borrowing {0} from {1} ...", KEY,
				FieldsPool.class.getName());

		try {
			return (MongoFields) pool.borrowObject(KEY);
		} catch (NoSuchElementException e) {
			logger.warn("No MongoFields in {0}.", FieldsPool.class.getName());
		} catch (IllegalStateException e) {
			logger.error(e, "Borrow MongoFields from {0} failed.",
					FieldsPool.class.getName());
		} catch (Exception e) {
			throw new RuntimeException(e);
		}

		return new MongoFields();
	}

	public FieldsPool returnFields(MongoFields f) {
		if(logger.isDebug())
            logger.debug("Returning {0} from {1} ...", KEY,
				FieldsPool.class.getName());
		
		try {
			f.reset();
			pool.returnObject(KEY, f);
		} catch (Exception e) {
			logger.error(e, "Return {0} failed!", KEY);
		}

		return this;
	}

	public void clear() {
		try {
			pool.clear(KEY);
		} catch (Exception e) {
			logger.error(e, "Clear {0} failed!", KEY);
		}
	}

	public int getMax() {
		return max;
	}

	public void setMax(int max) {
		this.max = max;
	}

}
