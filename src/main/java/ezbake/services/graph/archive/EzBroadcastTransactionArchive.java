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
import java.util.Properties;

import org.apache.thrift.TSerializer;

import ezbake.base.thrift.Visibility;
import ezbake.ezbroadcast.core.EzBroadcaster;
import ezbake.services.graph.thrift.Transaction;
import ezbake.services.graph.thrift.TransactionId;

/**
 * Implements a direct to Kafka transaction archive
 *
 * @author gzang
 */
public final class EzBroadcastTransactionArchive implements WriteTransactionArchive {

    public static final String ARCHIVE_TOPIC_DEFAULT = "transaction-archive";
    private EzBroadcaster broadcaster;

    public EzBroadcastTransactionArchive() {

    }

    /**
     * Constructor -- Inject the dependency, an EZBroadcaster object
     *
     * @param broadcaster EZBroadcaster object to be injected
     * @throws InterruptedException
     * @throws IOException
     */
    public EzBroadcastTransactionArchive(EzBroadcaster broadcaster) throws InterruptedException, IOException {
        registerBroadcaster(broadcaster);
    }

    private void registerBroadcaster(EzBroadcaster broadcaster) {
        this.broadcaster = broadcaster;
        broadcaster.registerBroadcastTopic(ARCHIVE_TOPIC_DEFAULT); // need register topic before broadcast to
    }

    @Override
    public void write(Transaction transaction) throws TransactionArchiveException {
        try {
            final TSerializer serializer = new TSerializer();
            final byte[] bsTrans = serializer.serialize(transaction);
            write(bsTrans, null, null, transaction.getVisibility().getFormalVisibility());
        } catch (final Exception ex) {
            throw new TransactionArchiveException("Exception in Thrift Serialization!", ex);
        }
    }

    @Override
    public void write(byte[] bsTrans, TransactionId tid, String apps, String visibility)
            throws TransactionArchiveException {
        try {
            broadcaster.broadcast(ARCHIVE_TOPIC_DEFAULT, new Visibility().setFormalVisibility(visibility), bsTrans);
        } catch (final IOException ex) {
            throw new TransactionArchiveException("Exception in broadcaster.broadcast!", ex);
        }
    }

    @Override
    public void close() throws IOException {
        broadcaster.close();
    }

    @Override
    public void init(Properties properties) throws TransactionArchiveException {
        final EzBroadcaster ezBroadcaster = EzBroadcaster.create(properties, "TA_group");
        // registerBroadcaster(ezBroadcaster);
    }
}
