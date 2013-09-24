/**
 * Copyright 2011 Jlue.org
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

import org.apache.commons.pool.KeyedPoolableObjectFactory;
import org.canedata.provider.mongodb.field.MongoFields;

/**
 * 
 * @author Sun Yat-ton (Mail:ImSunYitao@gmail.com)
 * @version 1.00.000 2011-9-1
 */
public class PoolableFactory implements KeyedPoolableObjectFactory {

	/* (non-Javadoc)
	 * @see org.apache.commons.pool.KeyedPoolableObjectFactory#activateObject(java.lang.Object, java.lang.Object)
	 */
	public void activateObject(Object arg0, Object arg1) throws Exception {
		
	}

	/* (non-Javadoc)
	 * @see org.apache.commons.pool.KeyedPoolableObjectFactory#destroyObject(java.lang.Object, java.lang.Object)
	 */
	public void destroyObject(Object arg0, Object arg1) throws Exception {
		String key = (String)arg0;
		if("mongoFields".equals(key)){
			((MongoFields)arg1).reset();
			
			return;
		}
	}

	/* (non-Javadoc)
	 * @see org.apache.commons.pool.KeyedPoolableObjectFactory#makeObject(java.lang.Object)
	 */
	public Object makeObject(Object arg0) throws Exception {
		return new MongoFields();
	}

	/* (non-Javadoc)
	 * @see org.apache.commons.pool.KeyedPoolableObjectFactory#passivateObject(java.lang.Object, java.lang.Object)
	 */
	public void passivateObject(Object arg0, Object arg1) throws Exception {
		String key = (String)arg0;
		if("mongoFields".equals(key)){
			((MongoFields)arg1).reset();
			
			return;
		}
	}

	/* (non-Javadoc)
	 * @see org.apache.commons.pool.KeyedPoolableObjectFactory#validateObject(java.lang.Object, java.lang.Object)
	 */
	public boolean validateObject(Object arg0, Object arg1) {
		return true;
	}

}
