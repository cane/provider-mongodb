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
package org.canedata.provider.mongodb.test.expr;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.logging.LogManager;

import org.canedata.expression.ParseExpressionException;
import org.canedata.provider.mongodb.expr.MongoExpression;
import org.canedata.provider.mongodb.expr.MongoExpressionBuilder;
import org.canedata.provider.mongodb.expr.MongoExpressionFactory;
import org.canedata.provider.mongodb.test.AbilityProvider;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * 
 * @author Sun Yat-ton
 * @version 1.00.000 2011-8-12
 */
public class TestExprFactory {
	static MongoExpressionFactory mef = new MongoExpressionFactory.Impl();
	static MongoExpressionBuilder builder;
	
	@BeforeClass
	public static void baseinit(){
		LogManager lm = LogManager.getLogManager();
		try {
			lm.readConfiguration(AbilityProvider.class
					.getResourceAsStream("/logging.properties"));
		} catch (SecurityException e) {
			throw e;
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
	
	@Test
	public void f() throws ParseExpressionException{
		builder = new MongoExpressionBuilder(null);
		MongoExpression equals = (MongoExpression)builder.equals("id", 1);
		assertEquals(mef.addExpression(equals).toQuery().toString(), "{\"id\": 1}");
		
		builder = new MongoExpressionBuilder(null);
		MongoExpression between = (MongoExpression)builder.reset().between("age", 18, 65);
		assertEquals(mef.reset().addExpression(between).toQuery().toString(), "{\"age\": {\"$gte\": 18, \"$lte\": 65}}");
		
		builder = new MongoExpressionBuilder(null);
		MongoExpression or = (MongoExpression)builder.reset().equals("id", 1).and().greaterThan("age", 18).or().lessThan("age", 65);
		assertEquals(mef.reset().addExpression(or).toQuery().toString(), "{\"$or\": [{\"id\": 1, \"age\": {\"$gt\": 18}}, {\"age\": {\"$lt\": 65}}]}");
		
		builder = new MongoExpressionBuilder(null);
		MongoExpression ore = (MongoExpression)builder.reset().in("name", new Object[]{"kiv", "ddv"}).or(between);
		assertEquals(mef.reset().addExpression(ore).toQuery().toString(), "{\"$or\": [{\"name\": {\"$in\": [\"kiv\", \"ddv\"]}}, {\"age\": {\"$gte\": 18, \"$lte\": 65}}]}");
	}
}
