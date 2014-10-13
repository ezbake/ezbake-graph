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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;

import ezbake.configuration.EzConfiguration;
import ezbake.services.graph.thrift.Transaction;
import ezbake.services.graph.thrift.TransactionId;

import ezbakehelpers.accumulo.AccumuloHelper;

/**
 * Implements a direct to Accumulo transaction archive.
 *
 * @author edeprit
 */
public final class AccumuloTransactionArchive implements ReadWriteTransactionArchive {
    private static final int MAX_BATCHWRITER_THREADS = 8;

    private static final int MAX_BATCHWRITER_MEMORY = 32 * 1024;
    private static final String TABLENAME_DEFAULT = "transaction_archive";
    private static final String APPLICATION_FAMILY = "Application";

    private Connector connector;
    private BatchWriter writer;
    private String tableName;
    private TSerializer tSerializer;
    private TDeserializer tDeserializer;

    public long byteInjected = 0;

    public AccumuloTransactionArchive() {
    }

    @Override
    public void init(Properties properties) throws TransactionArchiveException {
        final AccumuloHelper helper = new AccumuloHelper(properties);
        tableName = TABLENAME_DEFAULT;

        try {
            connector = helper.getConnector(false);

            if (!connector.tableOperations().exists(tableName)) {
                connector.tableOperations().create(tableName);
            }

            final BatchWriterConfig bwc = new BatchWriterConfig();
            bwc.setMaxMemory(MAX_BATCHWRITER_MEMORY);
            bwc.setMaxWriteThreads(MAX_BATCHWRITER_THREADS);
            bwc.setMaxLatency(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
            writer = connector.createBatchWriter(tableName, bwc);
        } catch (final TableNotFoundException ex) {
            throw new TransactionArchiveException(ex);
        } catch (final AccumuloException ex) {
            throw new TransactionArchiveException(ex);
        } catch (final AccumuloSecurityException ex) {
            throw new TransactionArchiveException(ex);
        } catch (final TableExistsException ex) {
            throw new TransactionArchiveException(ex);
        } catch (final IOException ex) {
            throw new TransactionArchiveException(ex);
        }

        tSerializer = new TSerializer();
        tDeserializer = new TDeserializer();
    }

    /**
     * Constructor -- create the connection to Accumulo, set the table name used for transaction archive to be default.
     * Thrift serializer and deserializer are also initialized.
     *
     * @param config Spring Framework Configuration that contains Accumulo Login info.
     * @throws TransactionArchiveException Exception may throw when connection to Accumulo fails.
     */
    public AccumuloTransactionArchive(EzConfiguration config) throws TransactionArchiveException {
        tableName = TABLENAME_DEFAULT;

        try {
            final AccumuloHelper helper = new AccumuloHelper(config.getProperties());
            connector = helper.getConnector(false);

            if (!connector.tableOperations().exists(tableName)) {
                connector.tableOperations().create(tableName);
            }

            final BatchWriterConfig bwc = new BatchWriterConfig();
            bwc.setMaxMemory(MAX_BATCHWRITER_MEMORY);
            bwc.setMaxWriteThreads(MAX_BATCHWRITER_THREADS);
            bwc.setMaxLatency(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
            writer = connector.createBatchWriter(tableName, bwc);
        } catch (final TableNotFoundException ex) {
            throw new TransactionArchiveException(ex);
        } catch (final AccumuloException ex) {
            throw new TransactionArchiveException(ex);
        } catch (final AccumuloSecurityException ex) {
            throw new TransactionArchiveException(ex);
        } catch (final TableExistsException ex) {
            throw new TransactionArchiveException(ex);
        } catch (final IOException ex) {
            throw new TransactionArchiveException(ex);
        }

        tSerializer = new TSerializer();
        tDeserializer = new TDeserializer();
    }

    /**
     * Returns a byte array of size 16, which contains the binary bits of two long integers contained in the
     * TransactionId structure -- Timestamp and ServerId.
     *
     * @param tid Transaction ID need to be converted.
     * @return byte array of size 16.
     */
    private byte[] convertTransIdToBS(TransactionId tid) {
        final ByteBuffer rowIdBuf = ByteBuffer.allocate(16);
        rowIdBuf.putLong(0, tid.getTimestamp());
        rowIdBuf.putLong(8, tid.getServerId());
        return rowIdBuf.array();
    }

    /**
     * Returns a Transaction object from Accumulo table for transaction archive, according to given transaction id.
     *
     * @param tid Given transaction id
     * @return the Transaction specified by the id
     * @throws TransactionArchiveException may throw when no table exist, or Thrift de-serialization failed.
     */
    @Override
    public Transaction read(TransactionId tid) throws TransactionArchiveException {
        try {
            final Scanner scanner = connector.createScanner(tableName, new Authorizations());
            final Range range = new Range(new Text(convertTransIdToBS(tid)));
            scanner.setRange(range);

            for (final Map.Entry<Key, Value> kv : scanner) {
                final Value value = kv.getValue();
                final byte[] bsTrans = value.get();
                final Transaction transaction = new Transaction();

                try {
                    tDeserializer.deserialize(transaction, bsTrans);
                    return transaction;
                } catch (final TException ex) {
                    throw new TransactionArchiveException(ex);
                }
            }

            return null;
        } catch (final TableNotFoundException ex) {
            throw new TransactionArchiveException(ex);
        }
    }

    /**
     * Return a collection of Transaction object in the given time range, from Accumulo transaction archive table
     *
     * @param startTime begin time for the search
     * @param endTime end time for the search
     * @return collection of Transaction object
     * @throws TransactionArchiveException may throw when no table exist, or Thrift de-serialization failed.
     */
    @Override
    public Iterable<Transaction> read(long startTime, long endTime) throws TransactionArchiveException {
        return read(startTime, endTime, new String[0]);
    }

    /**
     * Return a collection of Transaction object in the given time range and for the given application name(s), from
     * Accumulo transaction archive table
     *
     * @param startTime begin time for the search
     * @param endTime end time for the search
     * @param applications the given application name(s)
     * @return collection of Transaction object
     * @throws TransactionArchiveException may throw when no table exist, or Thrift de-serialization failed.
     */
    @Override
    public Iterable<Transaction> read(long startTime, long endTime, String... applications)
            throws TransactionArchiveException {
        try {
            final Scanner scanner = connector.createScanner(tableName, new Authorizations());

            for (final String application : applications) {
                // scanner.fetchColumnFamily( new Text( APPLICATION_FAMILY ) );
                scanner.fetchColumn(new Text(APPLICATION_FAMILY), new Text(application));
            }

            final TransactionId startId = new TransactionId(startTime, 0);
            final TransactionId endId = new TransactionId(endTime, -1); // Range is inclusive for both start and end
            final Range range = new Range(new Text(convertTransIdToBS(startId)), new Text(convertTransIdToBS(endId)));
            scanner.setRange(range);

            final ArrayList<Transaction> transactions = new ArrayList<>();
            for (final Map.Entry<Key, Value> kv : scanner) {
                final Value value = kv.getValue();
                final byte[] bsTrans = value.get();
                final Transaction transaction = new Transaction();

                try {
                    tDeserializer.deserialize(transaction, bsTrans);
                    transactions.add(transaction);
                } catch (final TException ex) {
                    throw new TransactionArchiveException(ex);
                }
            }

            return transactions;
        } catch (final TableNotFoundException ex) {
            throw new TransactionArchiveException(ex);
        }
    }

    /**
     * Write a transaction object into Accumulo transaction archive table.
     *
     * @param transaction Transaction to write
     * @throws TransactionArchiveException May be thrown if Thrift serialization failed.
     */
    @Override
    public void write(Transaction transaction) throws TransactionArchiveException {
        try {
            final byte[] bsTrans = tSerializer.serialize(transaction); // bit stream of transaction of thrift type.
            write(
                    bsTrans, transaction.getTid(), transaction.getApplication(),
                    transaction.getVisibility().getFormalVisibility());
        } catch (final Exception ex) {
            throw new TransactionArchiveException("Exception in Thrift Serialization!", ex);
        }
    }

    /**
     * Write a byte stream (containing a Thrift serialized transaction object) with its transaction Id and application
     * name into Accumulo transaction archive table
     *
     * @param bsTrans byte stream of transaction to write
     * @param tid transaction id
     * @param apps application name
     * @throws TransactionArchiveException my throw if Accumulo operations failed
     */
    @Override
    public void write(byte[] bsTrans, TransactionId tid, String apps, String visibility)
            throws TransactionArchiveException {
        try {

            final Mutation mutation = new Mutation(new Text(convertTransIdToBS(tid)));
            final ColumnVisibility vis = new ColumnVisibility(visibility); // for now, it is empty -- visible to every
            // one
            mutation.put(APPLICATION_FAMILY, apps, vis, new Value(bsTrans));

            writer.addMutation(mutation);
            writer.flush();

            byteInjected += bsTrans.length;
        } catch (final AccumuloException ex) {
            throw new TransactionArchiveException(ex);
        }
    }

    @Override
    public void close() throws IOException {
        try {
            writer.close();
        } catch (final Exception ex) {
            ex.printStackTrace();
        }
    }
}
