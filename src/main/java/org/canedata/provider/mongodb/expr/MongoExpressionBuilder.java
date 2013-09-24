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

import org.canedata.expression.Expression;
import org.canedata.expression.ExpressionBuilder;
import org.canedata.expression.shield.ExpressionA;
import org.canedata.expression.shield.ExpressionB;
import org.canedata.provider.mongodb.entity.MongoEntity;

/**
 * 
 * @author Sun Yat-ton
 * @version 1.00.000 2011-8-5
 */
public class MongoExpressionBuilder extends MongoExpressionB implements
		ExpressionBuilder {
	protected MongoEntity target = null;
	ThreadLocal<MongoExprIntent> lIntent = new ThreadLocal<MongoExprIntent>();
	protected MongoExpression mongoExpr = null;

	public MongoExpressionBuilder(MongoEntity entity) {
		target = entity;
	}

	@Override
	protected ExpressionA getExpressionA() {
		if (null == mongoExpr)
			mongoExpr = new MongoExpression() {

				@Override
				protected ExpressionB getExpressionB() {
					return MongoExpressionBuilder.this;
				}

				@SuppressWarnings("unchecked")
				@Override
				protected MongoExprIntent getIntent() {
					return MongoExpressionBuilder.this.getIntent();
				}

			};

		return mongoExpr;
	}

	@SuppressWarnings("unchecked")
	@Override
	protected MongoExprIntent getIntent() {
		MongoExprIntent i = lIntent.get();
		if(null == i){
			i = new MongoExprIntent();
			lIntent.set(i);
		}
		
		return i;
	}

	public ExpressionBuilder reset() {
		getIntent().reset();
		lIntent.set(null);

		return this;
	}

	public Expression build() {
		return getExpressionA();
	}

}
