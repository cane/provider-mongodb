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
package org.canedata.provider.mongodb.expr;

import org.canedata.core.field.expr.MongoOperator;
import org.canedata.core.intent.Step.AbstractStep;

import java.util.HashMap;
import java.util.Map;

/**
 * 
 * @author Sun Yat-ton
 * @version 1.00.000 2011-5-5
 */
public class MongoExprStep extends AbstractStep {
	public static final String[] names = new String[] { "eq", "neq", "lt",
			"lte", "gt", "gte", "bt", "nbt", "like", "nlike", "in", "nin",
			"et", "net", "nl", "nnl", "mch", "nmch", "and", "andn", "or", "orn", "all", "other" };

	static final Map<Integer, String> name_mapper = new HashMap(){{
		put(MongoOperator.EQUALS, "eq");
		put(MongoOperator.NOT_EQUALS, "neq");
		put(MongoOperator.LESSTHAN, "lt");
		put(MongoOperator.LESSTHAN_OR_EQUAL, "lte");
	}};

	/**
	 * @param step current step
	 * @param pur purpose
	 * @param scalar params
	 */
	public MongoExprStep(int step, String pur, Object[] scalar) {
		super(step, pur, scalar);
	}

	public String getName() {
		return name_mapper.get(step());
	}

}
