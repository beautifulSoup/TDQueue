package com.tangokk.tdqueue.core.conf;

public class ClusterConfigurationImpl implements ClusterConfiguration {


    private static ClusterConfiguration instance = null;

    String clusterName = "DEFAULT";


    public ClusterConfigurationImpl() {
    }

    public ClusterConfigurationImpl(String clusterName) {
        this.clusterName = clusterName;
    }


    public static ClusterConfiguration getInstance() {
        if(instance == null) {
            instance = new ClusterConfigurationImpl();
        }
        return instance;
    }


    @Override
    public String getClusterName() {
        return clusterName;
    }



}
