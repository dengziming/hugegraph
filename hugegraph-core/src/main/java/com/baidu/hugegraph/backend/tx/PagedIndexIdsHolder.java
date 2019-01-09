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
import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.Supplier;

import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.query.Query;
import com.baidu.hugegraph.util.E;

public final class PagedIndexIdsHolder extends IndexIdsHolder {

    private List<Id> ids;
    private String page;

    public PagedIndexIdsHolder(Query query, Supplier<Ids> idsFetcher) {
        super(query, idsFetcher);
        E.checkState(query.paging(),
                     "query '%s' must carry the page in pagination mode",
                     query);
        this.ids = null;
        this.resetPage();
    }

    private void resetPage() {
        this.page = "";
    }

    public String page() {
        return this.page;
    }

    @Override
    public Iterator<Id> iterator() {
        return new IdsIterator();
    }

    @Override
    public long size() {
        if (this.ids == null) {
            return 0;
        }
        return this.ids.size();
    }

    public class IdsIterator implements Iterator<Id> {

        private int cursor;
        private boolean finished;

        private IdsIterator() {
            this.cursor = 0;
            this.finished = false;
        }

        @Override
        public boolean hasNext() {
            // When local data is empty or consumed, then fetch from the backend
            if (ids == null || this.cursor >= ids.size()) {
                this.fetch();
            }
            assert ids != null;
            return this.cursor < ids.size();
        }

        private void fetch() {
            if (this.finished) {
                return;
            }

            query.page(page);
            Ids result = idsFetcher.get();
            E.checkState(result instanceof PagedIds,
                         "The result ids must be PagedIds for " +
                         "PagedIndexIdsHolder");
            ids = ((PagedIds) result).ids();
            page = ((PagedIds) result).page();
            this.cursor = 0;

            if (ids.size() != query.limit() || page == null) {
                this.finished = true;
            }
        }

        @Override
        public Id next() {
            if (!this.hasNext()) {
                throw new NoSuchElementException();
            }
            return ids.get(this.cursor++);
        }
    }
}
