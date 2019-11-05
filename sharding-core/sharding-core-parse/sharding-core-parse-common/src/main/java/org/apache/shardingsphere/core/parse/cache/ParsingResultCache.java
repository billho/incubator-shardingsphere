/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.shardingsphere.core.parse.cache;

import org.apache.commons.collections4.map.AbstractHashedMap;
import org.apache.commons.collections4.map.AbstractReferenceMap;
import org.apache.commons.collections4.map.ReferenceMap;
import org.apache.shardingsphere.core.parse.antlr.sql.statement.SQLStatement;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.Map;

/**
 * Parsing result cache.
 *
 * @author zhangliang
 * @author zhaojun
 */
public final class ParsingResultCache {

    /**
     * fix 因高并发引起的hasmap 扩容引起的进程死锁
     */
    private final Map<String, SQLStatement> cache =
//            Collections.synchronizedMap(
                    new ReferenceMap<String, SQLStatement>(AbstractReferenceMap.ReferenceStrength.SOFT, AbstractReferenceMap.ReferenceStrength.SOFT, 65535, 1)
//            )
            ;
    /**
     * Put SQL and parsing result into cache.
     * 
     * @param sql SQL
     * @param sqlStatement SQL statement
     */
    int mythreshold = 65000 ; //大概是 65536 * 0.9
    public void put(final String sql, final SQLStatement sqlStatement) {
        if (cache.size()>mythreshold) {
            this.mythreshold = getThreshold();
            synchronized (cache) {
                cache.put(sql, sqlStatement);
            }
        }
        else{
            cache.put(sql, sqlStatement);
        }
    }

    private int getThreshold(){
        try {
            Class clz = AbstractHashedMap.class;
            Field f = clz.getDeclaredField("threshold");
            f.setAccessible(true);
            return f.getInt(this.cache)-500;
        }
        catch ( Exception e){
            e.printStackTrace();
            return  mythreshold;
        }
    }
    
    /**
     * Get SQL statement.
     *
     * @param sql SQL
     * @return SQL statement
     */
    public SQLStatement getSQLStatement(final String sql) {
        return cache.get(sql);
    }
    
    /**
     * Clear cache.
     */
    public synchronized void clear() {
        cache.clear();
    }

    /**
     * test
     * @param args
     */
//    public static void main(String[] args) {
//        System.out.println(new ParsingResultCache().getThreshold());
//    }
}
