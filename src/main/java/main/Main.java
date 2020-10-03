package main;

import babel.Babel;
import babel.exceptions.HandlerRegistrationException;
import babel.exceptions.InvalidParameterException;
import babel.exceptions.ProtocolAlreadyExistsException;
import babel.generic.GenericProtocol;
import chainpaxos.*;
import chainreplication.ChainRepMixedFront;
import chainreplication.ChainRepMixedProto;
import distinguishedpaxos.DistPaxosFront;
import distinguishedpaxos.DistPaxosPiggyProto;
import distinguishedpaxos.DistPaxosProto;
import distinguishedpaxos.MultiPaxosProto;
import epaxos.EPaxosFront;
import epaxos.EPaxosProto;
import epaxos.EsolatedPaxosProto;
import frontend.FrontendProto;
import io.netty.channel.EventLoopGroup;
import network.NetworkManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ringpaxos.RingPaxosFront;
import ringpaxos.RingPaxosPiggyProto;
import ringpaxos.RingPaxosProto;

import java.io.IOException;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.*;

public class Main {

    private static final Logger logger = LogManager.getLogger(Main.class);

    public static void main(String[] args) throws IOException, InvalidParameterException,
            HandlerRegistrationException, ProtocolAlreadyExistsException {

        Babel babel = Babel.getInstance();
        Properties configProps = babel.loadConfig(args[0], Arrays.copyOfRange(args, 2, args.length));

        logger.debug(configProps);
        if (configProps.containsKey("interface")) {
            String address = getAddress(configProps.getProperty("interface"));
            if (address == null) return;
            configProps.setProperty(FrontendProto.ADDRESS_KEY, address);
            configProps.setProperty(ChainPaxosMixedProto.ADDRESS_KEY, address);
        }
        //translate interface name to address

        EventLoopGroup workerGroup = NetworkManager.createNewWorkerGroup();

        String alg = args[1];
        int nFrontends = Short.parseShort(configProps.getProperty("n_frontends"));

        List<FrontendProto> frontendProtos = new LinkedList<>();
        GenericProtocol consensusProto;
        if (alg.equals("chain_mixed")) {
            for (short i = 0; i < nFrontends; i++)
                frontendProtos.add(new ChainPaxosMixedFront(configProps, NetworkManager.createNewWorkerGroup(), i));
            consensusProto = new ChainPaxosMixedProto(configProps, workerGroup);
        } else if (alg.equals("chain_delayed")) {
            for (short i = 0; i < nFrontends; i++)
                frontendProtos.add(new ChainPaxosDelayedFront(configProps, NetworkManager.createNewWorkerGroup(), i));
            consensusProto = new ChainPaxosDelayedProto(configProps, workerGroup);
        } else if (alg.equals("chainrep")) {
            for (short i = 0; i < nFrontends; i++)
                frontendProtos.add(new ChainRepMixedFront(configProps, NetworkManager.createNewWorkerGroup(), i));
            consensusProto = new ChainRepMixedProto(configProps, workerGroup);
        } else if (alg.equals("distinguished")) {
            for (short i = 0; i < nFrontends; i++)
                frontendProtos.add(new DistPaxosFront(configProps, NetworkManager.createNewWorkerGroup(), i));
            consensusProto = new DistPaxosProto(configProps, workerGroup);
        } else if (alg.equals("distinguished_piggy")) {
            for (short i = 0; i < nFrontends; i++)
                frontendProtos.add(new DistPaxosFront(configProps, NetworkManager.createNewWorkerGroup(), i));
            consensusProto = new DistPaxosPiggyProto(configProps, workerGroup);
        } else if (alg.equals("multi")) {
            for (short i = 0; i < nFrontends; i++)
                frontendProtos.add(new DistPaxosFront(configProps, NetworkManager.createNewWorkerGroup(), i));
            consensusProto = new MultiPaxosProto(configProps, workerGroup);
        } else if (alg.equals("epaxos")) {
            for (short i = 0; i < nFrontends; i++)
                frontendProtos.add(new EPaxosFront(configProps, NetworkManager.createNewWorkerGroup(), i));
            consensusProto = new EPaxosProto(configProps, workerGroup);
        } else if (alg.equals("esolatedpaxos")) {
            for (short i = 0; i < nFrontends; i++)
                frontendProtos.add(new EPaxosFront(configProps, NetworkManager.createNewWorkerGroup(), i));
            consensusProto = new EsolatedPaxosProto(configProps, workerGroup);
        } else if (alg.equals("ring")) {
            for (short i = 0; i < nFrontends; i++)
                frontendProtos.add(new RingPaxosFront(configProps, NetworkManager.createNewWorkerGroup(), i));
            consensusProto = new RingPaxosProto(configProps, workerGroup);
        } else if (alg.equals("ringpiggy")) {
            for (short i = 0; i < nFrontends; i++)
                frontendProtos.add(new RingPaxosFront(configProps, NetworkManager.createNewWorkerGroup(), i));
            consensusProto = new RingPaxosPiggyProto(configProps, workerGroup);
        } else {
            logger.error("Unknown algorithm: " + alg);
            return;
        }

        for (FrontendProto frontendProto : frontendProtos)
            babel.registerProtocol(frontendProto);
        babel.registerProtocol(consensusProto);

        for (FrontendProto frontendProto : frontendProtos)
            frontendProto.init(configProps);
        consensusProto.init(configProps);

        babel.start();
    }

    private static String getAddress(String inter) throws SocketException {
        NetworkInterface byName = NetworkInterface.getByName(inter);
        if (byName == null) {
            logger.error("No interface named " + inter);
            return null;
        }
        Enumeration<InetAddress> addresses = byName.getInetAddresses();
        InetAddress currentAddress;
        while (addresses.hasMoreElements()) {
            currentAddress = addresses.nextElement();
            if (currentAddress instanceof Inet4Address)
                return currentAddress.getHostAddress();
        }
        logger.error("No ipv4 found for interface " + inter);
        return null;
    }
}
