package com.iwjw.kafka.clients.util;

import java.util.List;

public final class BrokerServerUtil {

    public static void checkBrokerServerList(List<String> brokerServerList){
        if(null == brokerServerList ||  brokerServerList.size() == 0){
            throw new IllegalArgumentException("brokerServerList is empty");
        }
        for(String brokerServer : brokerServerList){
            if(null == brokerServer || brokerServer.isEmpty()){
                throw new IllegalArgumentException("brokerServer is empty");
            }
            String[] hostPortPair = brokerServer.split(":");
            if(null == hostPortPair || hostPortPair.length != 2){
                throw new IllegalArgumentException(String.format("brokerServer %s format error please use host:port",brokerServer));
            }
        }
    }
    public static String getBrokerServerStr(List<String> brokerServerList){
        StringBuilder brokerServerBuilder = new StringBuilder(50);
        for(String brokerServer : brokerServerList){
            brokerServerBuilder.append(brokerServer).append(",");
        }
        return brokerServerBuilder.substring(0,brokerServerBuilder.length()-1).toString();
    }

}
