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

import java.io.Closeable;
import java.util.Properties;

/**
 * @author edeprit
 */
public interface WriteTransactionArchive extends Closeable {

    /**
     * Write a transaction object on to archive.
     *
     * @param transaction transaction to be writen
     * @throws TransactionArchiveException may throw when Thrift serialize the transaction object.
     */
    public void write(Transaction transaction) throws TransactionArchiveException;

    /**
     * An empty method, have to be implemented becasue of the inherited interface from WriteTransactionArchive
     *
     * @param bsTrans
     * @param tid
     * @param apps
     * @param visibility
     * @throws TransactionArchiveException
     */
    public void write(byte[] bsTrans, TransactionId tid, String apps, String visibility)
            throws TransactionArchiveException;

    /**
     * Perform any kind of initialization necessary.
     *
     * @param obj
     */
    public void init(Properties obj) throws TransactionArchiveException;
}
