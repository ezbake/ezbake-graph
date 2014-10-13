/*   Copyright (C) 2013-2014 Computer Sciences Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. */

package ezbake.services.graph.archive;

import ezbake.services.graph.thrift.Transaction;
import ezbake.services.graph.thrift.TransactionId;

/**
 * @author edeprit
 */
public interface ReadWriteTransactionArchive extends WriteTransactionArchive {

    public Transaction read(TransactionId tid) throws TransactionArchiveException;

    public Iterable<Transaction> read(long startTime, long endTime) throws TransactionArchiveException;

    public Iterable<Transaction> read(long startTime, long endTime, String... applications)
            throws TransactionArchiveException;
}
