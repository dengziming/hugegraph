/*
 * Copyright 2017 HugeGraph Authors
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package com.baidu.hugegraph.backend.tx;

import java.util.Iterator;
import java.util.Set;
import java.util.function.Supplier;

import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.query.Query;
import com.baidu.hugegraph.util.E;

public class EntireIndexIdsHolder extends IndexIdsHolder {

    private Set<Id> ids;

    public EntireIndexIdsHolder(Query query, Supplier<Ids> idsFetcher) {
        super(query, idsFetcher);
        this.ids = null;
    }

    public EntireIndexIdsHolder(Set<Id> ids) {
        super(null, null);
        this.ids = ids;
    }

    public void merge(EntireIndexIdsHolder holder) {
        this.all().addAll(holder.all());
    }

    public Set<Id> all() {
        if (this.ids == null) {
            Ids result = this.idsFetcher.get();
            E.checkState(result instanceof EntireIds,
                         "The result ids must be EntireIds for " +
                         "EntireIndexIdsHolder");
            this.ids = ((EntireIds) result).ids();
            assert this.ids != null;
        }
        return this.ids;
    }

    @Override
    public Iterator<Id> iterator() {
        return this.all().iterator();
    }

    @Override
    public long size() {
        return this.all().size();
    }
}
