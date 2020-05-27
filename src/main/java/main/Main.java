package main;

import babel.Babel;
import babel.exceptions.HandlerRegistrationException;
import babel.exceptions.InvalidParameterException;
import babel.exceptions.ProtocolAlreadyExistsException;
import babel.generic.GenericProtocol;
import chainpaxos.ChainPaxosProto;
import chainreplication.ChainReplicationProto;
import distinguishedpaxos.DistinguishedPaxosProto;
import distinguishedpaxos.MultiPaxosProto;
import epaxos.EPaxosProto;
import epaxos.EsolatedPaxosProto;
import frontend.FrontendProto;
import io.netty.channel.EventLoopGroup;
import network.NetworkManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ringpaxos.RingPaxosProto;

import java.io.IOException;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.Properties;

public class Main {

    private static final Logger logger = LogManager.getLogger(Main.class);

    public static void main(String[] args) throws IOException, InvalidParameterException,
            HandlerRegistrationException, ProtocolAlreadyExistsException {


        Babel babel = Babel.getInstance();
        Properties configProps = babel.loadConfig(args[0], Arrays.copyOfRange(args, 2, args.length));

        logger.debug(configProps);
        if(configProps.containsKey("interface")){
            String address = getAddress(configProps.getProperty("interface"));
            if(address == null) return;
            configProps.setProperty(FrontendProto.ADDRESS_KEY, address);
            configProps.setProperty(ChainPaxosProto.ADDRESS_KEY, address);
        }
        //translate interface name to address

        EventLoopGroup workerGroup = NetworkManager.createNewWorkerGroup();

        String alg = args[1];

        FrontendProto frontendProto = new FrontendProto(configProps, workerGroup);
        GenericProtocol consensusProto;
        if(alg.equals("chain"))
            consensusProto = new ChainPaxosProto(configProps, workerGroup);
        else if(alg.equals("chainrep"))
            consensusProto = new ChainReplicationProto(configProps, workerGroup);
        else if(alg.equals("distinguished"))
            consensusProto = new DistinguishedPaxosProto(configProps, workerGroup);
        else if(alg.equals("multi"))
            consensusProto = new MultiPaxosProto(configProps, workerGroup);
        else if(alg.equals("epaxos"))
            consensusProto = new EPaxosProto(configProps, workerGroup);
        else if(alg.equals("esolatedpaxos"))
            consensusProto = new EsolatedPaxosProto(configProps, workerGroup);
        else if(alg.equals("ring"))
            consensusProto = new RingPaxosProto(configProps, workerGroup);
        else {
            logger.error("Unknown algorithm: " + alg);
            return;
        }

        babel.registerProtocol(frontendProto);
        babel.registerProtocol(consensusProto);

        frontendProto.init(configProps);
        consensusProto.init(configProps);

        babel.start();
    }

    private static String getAddress(String inter) throws SocketException {
        NetworkInterface byName = NetworkInterface.getByName(inter);
        if(byName == null) {
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
