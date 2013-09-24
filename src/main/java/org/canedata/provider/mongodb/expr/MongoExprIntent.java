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

import java.util.Vector;

import org.canedata.core.intent.Intent;
import org.canedata.core.intent.Step;
import org.canedata.core.intent.Tracer;
import org.canedata.core.logging.LoggerFactory;
import org.canedata.exception.AnalyzeBehaviourException;
import org.canedata.logging.Logger;

/**
 * 
 * @author Sun Yat-ton
 * @version 1.00.000 2011-5-5
 */
public class MongoExprIntent implements Intent {
	Logger logger = LoggerFactory.getLogger(MongoExprIntent.class);

	protected int action = -1;
	protected Vector<Step> steps = new Vector<Step>();
	
	public Intent setAction(int action) {
		this.action = action;
		
		return this;
	}

	public Intent step(int stepId, String purpose, Object... scalars) {
		Step step = new MongoExprStep(stepId, purpose, scalars);
		
		logger.debug("Step to {0}, purpose is {1}, scalar is {2}.",
				step.getName(), purpose, (Object) scalars);
		
		steps.add(step);
		
		return this;
	}

	public int steps() {
		return steps.size();
	}

	public Intent playback(Tracer tracer) throws AnalyzeBehaviourException {
		for (Step step : steps) {
			tracer.trace(step);
		}
		return this;
	}

	public Intent reset() {
		action = -1;
		steps.clear();

		return this;
	}

}
