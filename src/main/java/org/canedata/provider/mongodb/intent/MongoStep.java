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
package org.canedata.provider.mongodb.intent;

import java.util.HashMap;
import java.util.Map;

import org.canedata.core.intent.Step.AbstractStep;

/**
 * 
 * @author Sun Yat-ton
 * @version 1.00.000 2011-5-1
 */
public class MongoStep extends AbstractStep {
	// steps
	public static final int NONE = -1;
	public static final int GET_ENTITY = 1;
	public static final int PUT = 2;

	public static final int CREATE = 30;
	public static final int RESTORE = 31;
	public static final int UPDATE = 32;
	public static final int DELETE = 33;
	public static final int QUERY = 34;
	public static final int LIST = 35;

	public static final int FETCH = 40;

	//
	public static final int PROJECTION = 50;
	public static final int FILTER = 51;
	public static final int LIMIT = 52;
	public static final int ORDER = 53;

	// function
	public static final int DISTINCT = 70;
	public static final int COUNT = 71;
	
	public static final int OPTION = 80;
	
	private static final Map<Integer, String> steps = new HashMap<Integer, String>();
	
	/**
	 * @param step
	 * @param pur
	 * @param scalar
	 */
	public MongoStep(int step, String pur, Object[] scalar) {
		super(step, pur, scalar);
	}

	public String getName() {
		return steps.get(step());
	}

}
