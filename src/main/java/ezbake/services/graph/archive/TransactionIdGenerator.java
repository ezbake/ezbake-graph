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

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ezbake.services.graph.archive;

import com.google.common.base.Preconditions;

import ezbake.services.graph.thrift.TransactionId;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;

/**
 * @author edeprit
 */
public class TransactionIdGenerator {

    private static final Logger logger = LoggerFactory.getLogger(TransactionIdGenerator.class);
    private static final String GEAR_UUID_VAR = "OPENSHIFT_GEAR_UUID";
    private static final String DEFAULT_UUID = "0123456789abcdef0123456789abcdef";

    public static final String INTERFACE_NAME_DEFAULT = "en0";
    public static final byte[] MAC_ADDRESS_DEFAULT = {0, 0, 0, 0, 0, 1};

    public static final int SEQUENCE_LEN = 16;
    public static final int SEQUENCE_MAX = (1 << SEQUENCE_LEN) - 1;

    private final long serverId;
    private long lastTime;
    private int sequence;

    /**
     * Default constructor uses the local host address to get the mac address. If there are multiple cards/vms running,
     * and you need to specify the ethernet interface name then use the alternate constructor.
     */
    public TransactionIdGenerator() {
        this(getServerID());
    }

    /**
     * Alternate constructor uses the passed in unique ID as the serverId.
     */
    public TransactionIdGenerator(long uniqueId) {
        serverId = uniqueId;
        lastTime = -1L;
        sequence = 0;
    }

    public TransactionIdGenerator(String interfaceName) {
        ByteBuffer buf = ByteBuffer.wrap(getMacAddress(interfaceName));
        serverId = buf.getInt();
        lastTime = -1L;
        sequence = 0;
    }

    public long getTime() {
        return System.currentTimeMillis();
    }

    public long getServerId() {
        return serverId;
    }

    public int getSequence(long serverId) {
        return (int) (serverId & SEQUENCE_MAX);
    }

    public synchronized TransactionId nextId() {
        long now = getTime();

        if (now < lastTime) {
            throw new IllegalStateException(String.format("Rejecting requests for %d ms.", lastTime - now));
        }

        if (now == lastTime) {
            sequence = (sequence + 1) & SEQUENCE_MAX;
            if (sequence == 0) {
                now = tilNextMillis(lastTime);
            }
        } else {
            sequence = 0;
        }

        lastTime = now;

        return new TransactionId(lastTime, (serverId << SEQUENCE_LEN) | sequence);
    }

    private long tilNextMillis(long lastTime) {
        long now = getTime();
        while (now <= lastTime) {
            now = getTime();
        }
        return now;
    }

    private static int getServerID() {
        byte[] serverId = null;

        try {
            serverId = getOpenShiftGearUUID();
        } catch (Exception e) {
            logger.info("Not on an OPENSHIFT machine, acquiring MOCK UUID");
            try {
                serverId = getDefaultServerId();
            } catch (DecoderException e1) {
                e1.printStackTrace();
                logger.info("Error acquiring valid UUID");
            }
        }

        Preconditions.checkNotNull(serverId);
        return ByteBuffer.wrap(serverId).getInt();
    }

    private static byte[] getOpenShiftGearUUID() throws DecoderException {
        logger.info("Using OpenShift GEAR UUID.");
        return getUUID(System.getenv(GEAR_UUID_VAR));
    }

    private static byte[] getDefaultServerId() throws DecoderException {
        logger.info("Using Mock UUID.");
        return getUUID(DEFAULT_UUID);
    }

    private static byte[] getUUID(String uuid) throws DecoderException {
        logger.info("Getting compressed UUID for " + uuid);
        Preconditions.checkNotNull(uuid);

        byte[] array = Hex.decodeHex(uuid.toCharArray());
        byte[] result = new byte[6];

        for (int i = 0; i < array.length; i++) {
            result[i % result.length] ^= array[i];
        }

        return result;
    }

    private static byte[] getMacAddress(String interfaceName) {
        Preconditions.checkArgument(interfaceName != null, "Interface name cannot be null.");
        try {
            NetworkInterface network = NetworkInterface.getByName(interfaceName);
            if (network == null) {
                throw new IllegalStateException("Can't get interface: " + interfaceName);
            }
            byte[] mac = network.getHardwareAddress();
            return (mac != null) ? mac : MAC_ADDRESS_DEFAULT;
        } catch (SocketException ex) {
            throw new IllegalStateException("Can't get MAC address: " + interfaceName);
        }
    }
}
