/*
 * Copyright (c) 2013-2016 Cisco Systems, Inc. and others.  All rights reserved.
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html
 *
 */

#include <iostream>
#include <string>
#include <list>
#include <cstring>
#include <cstdlib>
#include <unistd.h>

#include <arpa/inet.h>
#include <yaml-cpp/yaml.h>
#include <boost/xpressive/xpressive.hpp>
#include <boost/exception/all.hpp>
#include <boost/algorithm/string/replace.hpp>

#include "Config.h"
#include "pulsar/PulsarTopicSelector.h"

/*********************************************************************//**
 * Constructor for class
 ***********************************************************************/
Config::Config() {

    // Initialize the defaults
    bmp_port            = 5000;
    debug_general       = false;
    debug_bgp           = false;
    debug_bmp           = false;
    debug_msgbus        = false;
    bmp_buffer_size     = 15 * 1024 * 1024; // 15MB
    svr_ipv6            = false;
    svr_ipv4            = true;
    bind_ipv4           = "";
    bind_ipv6           = "";
    heartbeat_interval  = 60 * 5;        // Default is 5 minutes
    brokers             = "localhost:9092";
    tx_max_bytes        = 1000000;
    rx_max_bytes        = 100000000;
    socket_timeout	    = 60000; 	// Default is 60 seconds
    q_buf_max_msgs      = 100000;
    q_buf_max_kbytes    = 1048576;
    q_buf_max_ms        = 1000;         // Default is 1 sec
    msg_send_max_retry  = 2;
    retry_backoff_ms    = 100;
    compression         = "snappy";
    max_concurrent_routers = 2;
    initial_router_time = 60;
    calculate_baseline  = true;
    pat_enabled		= false;
    bzero(admin_id, sizeof(admin_id));

    dummy_producer_enabled = false;

    /*
     * Initialized the topic names
     *      The keys match the configuration node/vars. Topic name nodes will be ignored if
     *      not initialized here.
     */
    topic_names_map[MSGBUS_TOPIC_VAR_COLLECTOR]        = MSGBUS_TOPIC_COLLECTOR;
    topic_names_map[MSGBUS_TOPIC_VAR_ROUTER]           = MSGBUS_TOPIC_ROUTER;
    topic_names_map[MSGBUS_TOPIC_VAR_PEER]             = MSGBUS_TOPIC_PEER;
    topic_names_map[MSGBUS_TOPIC_VAR_BMP_STAT]         = MSGBUS_TOPIC_BMP_STAT;
    topic_names_map[MSGBUS_TOPIC_VAR_BMP_RAW]          = MSGBUS_TOPIC_BMP_RAW;
    topic_names_map[MSGBUS_TOPIC_VAR_BASE_ATTRIBUTE]   = MSGBUS_TOPIC_BASE_ATTRIBUTE;
    topic_names_map[MSGBUS_TOPIC_VAR_UNICAST_PREFIX]   = MSGBUS_TOPIC_UNICAST_PREFIX;
    topic_names_map[MSGBUS_TOPIC_VAR_LS_NODE]          = MSGBUS_TOPIC_LS_NODE;
    topic_names_map[MSGBUS_TOPIC_VAR_LS_LINK]          = MSGBUS_TOPIC_LS_LINK;
    topic_names_map[MSGBUS_TOPIC_VAR_LS_PREFIX]        = MSGBUS_TOPIC_LS_PREFIX;
    topic_names_map[MSGBUS_TOPIC_VAR_L3VPN]            = MSGBUS_TOPIC_L3VPN;
    topic_names_map[MSGBUS_TOPIC_VAR_EVPN]             = MSGBUS_TOPIC_EVPN;
}

/*********************************************************************//**
 * Load configuration from file in YAML format
 *
 * \param [in] cfg_filename     Yaml configuration filename
 ***********************************************************************/
void Config::load(const char *cfg_filename) {

    if (debug_general)
        std::cout << "---| Loading configuration file |----------------------------- " << std::endl;

    try {
        YAML::Node root = YAML::LoadFile(cfg_filename);

        /*
         * Iterate through the root node objects - We expect only maps at the root level, skip others
         */
        if (root.Type() == YAML::NodeType::Map) {
            for (YAML::const_iterator it = root.begin(); it != root.end(); ++it) {
                const YAML::Node &node = it->second;
                const std::string &key = it->first.Scalar();

                if (node.Type() == YAML::NodeType::Map) {
                    if (key.compare("base") == 0)
                        parseBase(node);
                    else if (key.compare("debug") == 0)
                        parseDebug(node);
                    else if (key.compare("pulsar") == 0)
                        parsePulsar(node);
                    else if (key.compare("mapping") == 0)
                        parseMapping(node);

                    else if (debug_general)
                        std::cout << "   Config: Key " << key << " Type " << node.Type() << std::endl;
                }
                else {
                    printWarning("configuration should only have maps at the root/base level found", node);
                }
            }
        } else {
            printWarning("configuration should only have maps at the root/base level found", root);

        }

    } catch (YAML::BadFile err) {
        throw err.what();
    } catch (YAML::ParserException err) {
        throw err.what();
    } catch (YAML::InvalidNode err) {
        throw err.what();
    }

    if (debug_general)
        std::cout << "---| Done Loading configuration file |------------------------- " << std::endl;
}

/**
 * Parse the base configuration
 *
 * \param [in] node     Reference to the yaml NODE
 */
void Config::parseBase(const YAML::Node &node) {
    std::string value;

    if (node["admin_id"]) {
        try {
            value = node["admin_id"].as<std::string>();

            if (value.compare("hostname") == 0) {
                gethostname(admin_id, sizeof(admin_id));
            } else {
                std::strncpy(admin_id, value.c_str(), sizeof(admin_id));
            }

            if (debug_general)
                std::cout << "   Config: admin id : " << admin_id << std::endl;

        } catch (YAML::TypedBadConversion<std::string> err) {
            printWarning("admin_id is not of type string", node["admin_id"]);
        }
    }

    if (node["listen_port"]) {
        try {
            bmp_port = node["listen_port"].as<uint16_t>();

            if (bmp_port < 25 || bmp_port > 65535)
                throw "invalid listen_port, not within range of 25 - 65535)";

            if (debug_general)
                std::cout << "   Config: bmp_port: " << bmp_port << std::endl;

        } catch (YAML::TypedBadConversion<uint16_t> err) {
            printWarning("bmp_port is not of type unsigned 16 bit", node["listen_port"]);
        }
    }

    if (node["listen_ipv4"]) {
        bind_ipv4 = node["listen_ipv4"].as<std::string>();

        if (debug_general)
            std::cout << "   Config: listen_ipv4: " << bind_ipv4 << "\n";
    }

    if (node["listen_ipv6"]) {
        bind_ipv6 = node["listen_ipv6"].as<std::string>();

        if (debug_general)
            std::cout << "   Config: listen_ipv6: " << bind_ipv6 << "\n";
    }

    if (node["listen_mode"]) {
        try {
            value = node["listen_mode"].as<std::string>();

            if (value.compare("v4") == 0) {
                svr_ipv4 = true;
            } else if (value.compare("v6") == 0) {
                svr_ipv6 = true;
                svr_ipv4 = false;
            } else { /* don't care if it's v4v6 or not */
                svr_ipv6 = true;
                svr_ipv4 = true;
            }

            if (debug_general)
                std::cout <<  "   Config: listen_mode is " << value << std::endl;

        } catch (YAML::TypedBadConversion<std::string> err) {
            printWarning("listen_mode is not of type string", node["listen_mode"]);
        }
    }

    if (node["buffers"]) {
        if (node["buffers"]["router"]) {
            try {
                bmp_buffer_size = node["buffers"]["router"].as<int>();

                if (bmp_buffer_size < 2 || bmp_buffer_size > 384)
                    throw "invalid router buffer size, not within range of 2 - 384)";

                bmp_buffer_size *= 1024 * 1024;  // MB to bytes

                if (debug_general)
                    std::cout << "   Config: bmp buffer: " << bmp_buffer_size << std::endl;

            } catch (YAML::TypedBadConversion<int> err) {
                printWarning("buffers.router is not of type int", node["buffers"]["router"]);
            }
        }
    }

    if (node["heartbeat"]) {
        if (node["heartbeat"]["interval"]) {
            try {
                heartbeat_interval = node["heartbeat"]["interval"].as<int>();

                if (heartbeat_interval < 1 || heartbeat_interval > 1440)
                    throw "invalid heartbeat interval not within range of 1 - 1440)";

                heartbeat_interval *= 60;   // minutes to seconds

                if (debug_general)
                    std::cout << "   Config: heartbeat interval: " << heartbeat_interval << std::endl;

            } catch (YAML::TypedBadConversion<int> err) {
                printWarning("heartbeat.router is not of type int", node["heartbeat"]["interval"]);
            }
        }
    }

    if (node["startup"]) {
        if (node["startup"]["max_concurrent_routers"]) {
            try {
                max_concurrent_routers = node["startup"]["max_concurrent_routers"].as<int>();

		if (max_concurrent_routers == 0)
		    max_concurrent_routers = MAX_THREADS;

                else if (max_concurrent_routers < 0)
                    throw "invalid maximum concurrent routers not greater than 0)";

                if (debug_general)
                    std::cout << "   Config: max concurrent routers: " << max_concurrent_routers << std::endl;

            } catch (YAML::TypedBadConversion<int> err) {
                printWarning("max_concurrent_routers is not of type int", node["startup"]["max_concurrent_routers"]);
            }
        }

        if (node["startup"]["initial_router_time"]) {
            try {
                initial_router_time = node["startup"]["initial_router_time"].as<int>();

                if (initial_router_time < 5 || initial_router_time > 1800) 
                    throw "invalid initial router time not within range of 5-1800 seconds)";

                if (debug_general)
                    std::cout << "   Config: Initial Router Time: " << initial_router_time << std::endl;

            } catch (YAML::TypedBadConversion<int> err) {
                printWarning("initial_router_time is not of type int", node["startup"]["initial_router_time"]);
            }
        }

        if (node["startup"]["calculate_baseline"]) {
            try {
                calculate_baseline = node["startup"]["calculate_baseline"].as<bool>();

                if (debug_general)
                    std::cout << "   Config: calculate_baseline: " << calculate_baseline << std::endl;

            } catch (YAML::TypedBadConversion<bool> err) {
                printWarning("calculate_baseline is not of type bool", node["startup"]["calculate_baseline"]);
            }
        }

        if (node["startup"]["pat_enabled"]) {
            try {
                pat_enabled = node["startup"]["pat_enabled"].as<bool>();

                if (debug_general)
                    std::cout << "   Config: pat_enabled: " << calculate_baseline << std::endl;

            } catch (YAML::TypedBadConversion<bool> err) {
                printWarning("pat_enabled is not of type bool", node["startup"]["pat_enabled"]);
            }
        }
    }

}

/**
 * Parse the debug configuration
 *
 * \param [in] node     Reference to the yaml NODE
 */
void Config::parseDebug(const YAML::Node &node) {
    if (!debug_general and node["general"]) {
        try {
            debug_general = node["general"].as<bool>();

            if (debug_general)
                std::cout << "   Config: debug general : " << debug_general << std::endl;

        } catch (YAML::TypedBadConversion<bool> err) {
            printWarning("debug.general is not of type boolean", node["general"]);
        }
    }

    if (!debug_bmp and node["bmp"]) {
        try {
            debug_bmp = node["bmp"].as<bool>();

            if (debug_general)
                std::cout << "   Config: debug bmp : " << debug_bmp << std::endl;

        } catch (YAML::TypedBadConversion<bool> err) {
            printWarning("debug.bmp is not of type boolean", node["bmp"]);
        }
    }

    if (!debug_bgp and node["bgp"]) {
        try {
            debug_bgp = node["bgp"].as<bool>();

            if (debug_general)
                std::cout << "   Config: debug bgp : " << debug_bgp << std::endl;

        } catch (YAML::TypedBadConversion<bool> err) {
            printWarning("debug.bgp is not of type boolean", node["bgp"]);
        }
    }

    if (!debug_msgbus and node["msgbus"]) {
        try {
            debug_msgbus = node["msgbus"].as<bool>();

            if (debug_general)
                std::cout << "   Config: debug msgbus : " << debug_msgbus << std::endl;

        } catch (YAML::TypedBadConversion<bool> err) {
            printWarning("debug.msgbus is not of type boolean", node["msgbus"]);
        }
    }
}

/**
 * Parse the pulsar configuration
 *
 * \param [in] node     Reference to the yaml NODE
 */
void Config::parsePulsar(const YAML::Node &node) {
    std::string value;

    if (node["brokers"] && node["brokers"].Type() == YAML::NodeType::Sequence) {
        brokers.clear();

        for (std::size_t i = 0; i < node["brokers"].size(); i++) {
            value = node["brokers"][i].Scalar();

            if (value.size() > 0) {
                if (brokers.size() > 0)
                    brokers.append(",");

                brokers.append(value);
            }

            if (debug_general)
                std::cout << "   Config: pulsar.brokers = " << brokers << "\n";
        }
    }

    if (!dummy_producer_enabled and node["dummy.producer.enabled"]) {
        try {
            dummy_producer_enabled = node["dummy.producer.enabled"].as<bool>();

            if (dummy_producer_enabled)
                std::cout << "   Config: dummy.producer.enabled : " << dummy_producer_enabled << std::endl;

        } catch (YAML::TypedBadConversion<bool> err) {
            printWarning("dummy.producer.enabled is not of type boolean", node["dummy.producer.enabled"]);
        }
    }

    if (node["message.max.bytes"] && node["message.max.bytes"].Type() == YAML::NodeType::Scalar) {
       try {
            tx_max_bytes = node["message.max.bytes"].as<int>();

            if (tx_max_bytes < 1000 || tx_max_bytes > 1000000000)
                throw "invalid transmit max bytes , should be "
		"in range 1000 - 1000000000";

           // Below corrects older configs to use 1M instead of 200MB.
           if (tx_max_bytes == 200000000)
               tx_max_bytes = 1000000;

            if (debug_general)
                std::cout << "   Config: transmit max bytes : " << tx_max_bytes 
		 << std::endl;

        } catch (YAML::TypedBadConversion<int> err) {
                printWarning("message.max.bytes is not of type int", 
				node["message.max.bytes"]);
        }
    }

    if (node["receive.message.max.bytes"]  && node["receive.message.max.bytes"].Type() == YAML::NodeType::Scalar) {
        try {
            rx_max_bytes = node["receive.message.max.bytes"].as<int>();

            if (rx_max_bytes < 1000 || rx_max_bytes > 1000000000)
               throw "invalid receive max bytes , should be "
				"in range 1000 - 1000000000";
            if (debug_general)
                   std::cout << "   Config: receive max bytes : " << rx_max_bytes
				 << std::endl;

        } catch (YAML::TypedBadConversion<int> err) {
                printWarning("receive.message.max.bytes is not of type int", 
				node["receive.message.max.bytes"]);
        }
    }

    if (node["socket.timeout.ms"]  &&
        node["socket.timeout.ms"].Type() == YAML::NodeType::Scalar) {
        try {
            socket_timeout = node["socket.timeout.ms"].as<int>();

            if (socket_timeout < 10 || socket_timeout > 300000)
               throw "invalid receive max bytes , should be "
				"in range 10 - 300000";
            if (debug_general)
                   std::cout << "   Config: socket timeout in ms: " << socket_timeout
				 << std::endl;

        } catch (YAML::TypedBadConversion<int> err) {
                printWarning("socket_timeout is not of type int", 
				node["socket.timeout.ms"]);
        }
    }

    if (node["queue.buffering.max.messages"]  && 
        node["queue.buffering.max.messages"].Type() == YAML::NodeType::Scalar) {
        try {
            q_buf_max_msgs = node["queue.buffering.max.messages"].as<int>();

            if (q_buf_max_msgs < 1 || q_buf_max_msgs > 10000000)
               throw "invalid receive max bytes , should be "
				"in range 1 - 10000000";
            if (debug_general)
                   std::cout << "   Config: queue buffering max messages: " << 
                                q_buf_max_msgs << std::endl;

        } catch (YAML::TypedBadConversion<int> err) {
                printWarning("q_buf_max_msgs is not of type int", 
				node["queue.buffering.max.messages"]);
        }
    }

    if (node["queue.buffering.max.kbytes"]  &&
        node["queue.buffering.max.kbytes"].Type() == YAML::NodeType::Scalar) {
        try {
            q_buf_max_kbytes = node["queue.buffering.max.kbytes"].as<int>();

            if (q_buf_max_kbytes < 1 || q_buf_max_kbytes > 2097151)
                throw "invalid receive max bytes , should be "
                        "in range 1 - 2097151";
            if (debug_general)
                std::cout << "   Config: queue buffering max kbytes: " <<
                          q_buf_max_kbytes << std::endl;

        } catch (YAML::TypedBadConversion<int> err) {
            printWarning("q_buf_max_kbytes is not of type int",
                         node["queue.buffering.max.kbytes"]);
        }
    }


    if (node["queue.buffering.max.ms"]  && 
        node["queue.buffering.max.ms"].Type() == YAML::NodeType::Scalar) {
        try {
            q_buf_max_ms = node["queue.buffering.max.ms"].as<int>();

            if (q_buf_max_ms < 1 || q_buf_max_ms > 900000)
               throw "invalid receive max bytes , should be "
				"in range 1 - 900000";
            if (debug_general)
                   std::cout << "   Config: queue buffering max time in ms: " << 
                                q_buf_max_ms << std::endl;

        } catch (YAML::TypedBadConversion<int> err) {
                printWarning("q_buf_max_ms is not of type int", 
				node["queue.buffering.max.ms"]);
        }
    }

    if (node["message.send.max.retries"]  && 
        node["message.send.max.retries"].Type() == YAML::NodeType::Scalar) {
        try {
            msg_send_max_retry = node["message.send.max.retries"].as<int>();

            if (msg_send_max_retry < 0 || msg_send_max_retry > 10000000)
               throw "invalid receive max bytes , should be "
				"in range 1 - 10000000";
            if (debug_general)
                   std::cout << "   Config: max message send retry: " << 
                                msg_send_max_retry << std::endl;

        } catch (YAML::TypedBadConversion<int> err) {
                printWarning("msg_send_max_retry is not of type int", 
				node["message.send.max.retries"]);
        }
    }

    if (node["retry.backoff.ms"]  && 
        node["retry.backoff.ms"].Type() == YAML::NodeType::Scalar) {
        try {
            retry_backoff_ms = node["retry.backoff.ms"].as<int>();

            if (retry_backoff_ms < 1 || retry_backoff_ms > 300000)
               throw "invalid receive max bytes , should be "
				"in range 1 - 300000";
            if (debug_general)
                   std::cout << "   Config: backoff time before resending failed message in ms: " << 
                                retry_backoff_ms << std::endl;

        } catch (YAML::TypedBadConversion<int> err) {
                printWarning("retry_backoff_ms is not of type int", 
				node["retry.backoff.ms"]);
        }
    }

    if (node["compression.codec"]  && 
        node["compression.codec"].Type() == YAML::NodeType::Scalar) {
        try {
            compression = node["compression.codec"].as<std::string>();

            if (compression != "none" && compression != "snappy" && 
                compression != "gzip" && compression != "lz4")
               throw "invalid value for compression, should be one of none,"
			" gzip, snappy, or lz4";
            if (debug_general)
                   std::cout << "   Config: Compression : " << 
                                compression << std::endl;

        } catch (YAML::TypedBadConversion<std::string> err) {
                printWarning("Compression is not of type string", 
				node["compression.codec"]);
        }
    }

    // default pulsar topic prefix
    topic_prefix="persistent://public/default/";
    
    if (node["topic.prefix"]  && 
        node["topic.prefix"].Type() == YAML::NodeType::Scalar) {
        try {
            if (topic_prefix.size()  > 0 )
                topic_prefix = node["topic.prefix"].as<std::string>();

        } catch (YAML::TypedBadConversion<std::string> err) {
                printWarning("topic.prefix is not of type string", 
				node["topic.prefix"]);
        }
    }
    if (debug_general)
        std::cout << "   Config: topic.prefix : " << 
             topic_prefix << std::endl;


    if (node["topics"] && node["topics"].Type() == YAML::NodeType::Map) {
        parseTopics(node["topics"]);
    }
    
}



/**
 * Parse the pulsar topics configuration
 *
 * \param [in] node     Reference to the yaml NODE
 */
void Config::parseTopics(const YAML::Node &node) {

    if (node["variables"] and node["variables"].Type() == YAML::NodeType::Map) {
        for (YAML::const_iterator it = node["variables"].begin(); it != node["variables"].end(); ++it) {
            try {
                const std::string &var = it->first.as<std::string>();

                // make sure user-defined variable doesn't override app specific ones
                if (var.compare("router_group") and var.compare("peer_group"))
                    topic_vars_map[var] = it->second.as<std::string>();

            } catch (YAML::TypedBadConversion<std::string> err) {
                printWarning("pulsar.topics.variables error in map.  Make sure to define var: <string value>", it->second);
            }
        }

        if (debug_general) {
            for (topic_vars_map_iter it = topic_vars_map.begin(); it != topic_vars_map.end(); ++it) {
                std::cout << "   Config: pulsar.topics.variables: " << it->first << " = " << it->second << std::endl;
            }
        }
    }

    if (node["names"] and node["names"].Type() == YAML::NodeType::Map) {
        for (YAML::const_iterator it = node["names"].begin(); it != node["names"].end(); ++it) {
            try {
                // Only add topic names that are initialized, otherwise ignore them
                if (topic_names_map.find(it->first.as<std::string>()) != topic_names_map.end()) {
                    if (it->second.Type() == YAML::NodeType::Null) {
                        topic_names_map[it->first.as<std::string>()] = "";
                        std::cout << "   Ignore: '" << it->first.as<std::string>()
                              << "' is disabled" << std::endl;
                    } else {
                        topic_names_map[it->first.as<std::string>()] = it->second.as<std::string>();
                    }
                } else if (debug_general)
                    std::cout << "   Ignore: '" << it->first.as<std::string>()
                              << "' is not a valid topic name entry" << std::endl;


            } catch (YAML::TypedBadConversion<std::string> err) {
                printWarning("pulsar.topics.names error in map.  Make sure to define var: <string value>", it->second);
            }
        }

        if (debug_general) {
            for (topic_names_map_iter it = topic_names_map.begin(); it != topic_names_map.end(); ++it) {
                std::cout << "   Config: pulsar.topics.names: " << it->first << " = " << it->second << std::endl;
            }
        }
    }

    // Update the topics based on user-defined variables
    topicSubstitutions();

    if (debug_general) {
        for (topic_names_map_iter it = topic_names_map.begin(); it != topic_names_map.end(); ++it) {
            std::cout << "   Config: postsub: pulsar.topics.names: " << it->first << " = " << it->second << std::endl;
        }
    }
}

/**
 * Parse the mapping configuration
 *
 * \param [in] node     Reference to the yaml NODE
 */
void Config::parseMapping(const YAML::Node &node) {
    if (node["groups"] and node["groups"].Type() == YAML::NodeType::Map) {

        if (node["groups"]["router_group"] and node["groups"]["router_group"].Type() == YAML::NodeType::Sequence) {

            std::string name;
            for (std::size_t i = 0; i < node["groups"]["router_group"].size(); i++) {

                if (node["groups"]["router_group"][i].Type() == YAML::NodeType::Map) {
                    const YAML::Node &cur_node = node["groups"]["router_group"][i];

                    name = cur_node["name"].as<std::string>();

                    if (debug_general)
                        std::cout << "   Config: mappings.groups.router_group name = " << name << std::endl;

                    if (debug_general) std::cout << "   Config: getting regexp_hostname list" << std::endl;
                    if (cur_node["regexp_hostname"] and
                        cur_node["regexp_hostname"].Type() == YAML::NodeType::Sequence) {

                        parseRegexpList(cur_node["regexp_hostname"], name, match_router_group_by_name);

                    } else if (cur_node["regexp_hostname"])
                        throw "Invalid mapping.groups.router_group.regexp_hostname, should be of type list/sequence";


                    if (debug_general) std::cout << "   Config: getting prefix_range list" << std::endl;
                    if (cur_node["prefix_range"] and cur_node["prefix_range"].Type() == YAML::NodeType::Sequence) {

                        parsePrefixList(cur_node["prefix_range"], name, match_router_group_by_ip);

                    } else if (cur_node["prefix_range"])
                        throw "Invalid mapping.groups.router_group.prefix_range, should be of type list/sequence";
                }
            }
        }

        if (node["groups"]["peer_group"] and node["groups"]["peer_group"].Type() == YAML::NodeType::Sequence) {

            std::string name;
            for (std::size_t i = 0; i < node["groups"]["peer_group"].size(); i++) {

                if (node["groups"]["peer_group"][i].Type() == YAML::NodeType::Map) {
                    const YAML::Node &cur_node = node["groups"]["peer_group"][i];

                    name = cur_node["name"].as<std::string>();

                    if (debug_general)
                        std::cout << "   Config: mappings.groups.peer_group name = " << name << std::endl;

                    if (debug_general) std::cout << "   Config: getting regexp_hostname list" << std::endl;
                    if (cur_node["regexp_hostname"] and
                        cur_node["regexp_hostname"].Type() == YAML::NodeType::Sequence) {

                        parseRegexpList(cur_node["regexp_hostname"], name, match_peer_group_by_name);

                    } else if (cur_node["regexp_hostname"])
                        throw "Invalid mapping.groups.peer_group.regexp_hostname, should be of type list/sequence";


                    if (debug_general) std::cout << "   Config: getting prefix_range list" << std::endl;
                    if (cur_node["prefix_range"] and cur_node["prefix_range"].Type() == YAML::NodeType::Sequence) {

                        parsePrefixList(cur_node["prefix_range"], name, match_peer_group_by_ip);

                    } else if (cur_node["prefix_range"])
                        throw "Invalid mapping.groups.peer_group.prefix_range, should be of type list/sequence";

                    if (debug_general) std::cout << "   Config: getting asn list" << std::endl;
                    if (cur_node["asn"] and cur_node["asn"].Type() == YAML::NodeType::Sequence) {

                        for (std::size_t i = 0; i < cur_node["asn"].size(); i++) {

                            if (cur_node["asn"][i].Type() == YAML::NodeType::Scalar) {
                                try {
                                    uint32_t asn = cur_node["asn"][i].as<std::uint32_t>();
                                    match_peer_group_by_asn[name].push_back(asn);
                                } catch (YAML::TypedBadConversion<std::string> err) {
                                    printWarning(
                                            "mapping.groups.peer_group.asn int parse error. ASN must be uint32: ",
                                            cur_node["asn"][i]);
                                }
                            }
                        }

                    } else if (cur_node["asn"])
                        throw "Invalid mapping.groups.peer_group.asn, should be of type list/sequence";

                }
            }
        }

    }
}

/**
 * Parse matching regexp list and update the provided map with compiled expressions
 *
 * \param [in]  node     regex list node - should be of type sequence
 * \param [in]  name     group name, used as the map key
 * \param [out] map      Reference to the map that will be updated with the compiled expressions
 */
void Config::parseRegexpList(const YAML::Node &node, std::string name,
                             std::map<std::string, std::list<match_type_regex>> &map) {

    match_type_regex value;

    for (std::size_t i = 0; i < node.size(); i++) {
        if (node[i].Type() == YAML::NodeType::Scalar) {

            try {
                value.regexp = sregex::compile(node[i].as<std::string>(),
                                               regex_constants::icase | regex_constants::not_dot_newline
                                               | regex_constants::optimize | regex_constants::nosubs);
                map[name].push_back(value);

            } catch (boost::exception_detail::clone_impl<boost::xpressive::regex_error> err) {
                throw "Invalid regular expression pattern";
            }

            if (debug_general)
                std::cout << "   Config: compiled regexp hostname: " << node[i].as<std::string>() << std::endl;
        }
    }
}

/**
 * Parse matching prefix_range list and update the provided map with compiled expressions
 *
 * \param [in]  node     prefix_range list node - should be of type sequence
 * \param [in]  name     group name, used as the map key
 * \param [out] map      Reference to the map that will be updated with ip addresses
 */
void Config::parsePrefixList(const YAML::Node &node, std::string name,
                             std::map<std::string, std::list<match_type_ip>> &map) {

    match_type_ip value;
    char *prefix_full;
    char *prefix, *bits;

    for (std::size_t i = 0; i < node.size(); i++) {
        bzero(value.prefix, sizeof(value.prefix));

        if (node[i].Type() == YAML::NodeType::Scalar) {

            // Split the prefix/bits
            prefix_full = strdup(node[i].as<std::string>().c_str());

            if (debug_general)
                std::cout << "   Config: parsing prefix range entry: " << prefix_full << std::endl;

            prefix = strtok(prefix_full, "/");
            bits = strtok(NULL, "/");

            if (prefix == NULL or bits == NULL)
                throw "Missing prefix range bits value";

            value.bits = atoi(bits);

            if (node[i].as<std::string>().find_first_of(".") != std::string::npos) {
                value.isIPv4 = true;

                if (value.bits < 1 or value.bits > 32)
                    throw "Invalid prefix range bits value, must be 1 - 32";
            } else {
                value.isIPv4 = false;

                if (value.bits < 1 or value.bits > 128)
                    throw "Invalid prefix range bits value, must be 1 - 128";
            }



            // add the inet address
            inet_pton((value.isIPv4 ? AF_INET : AF_INET6), prefix, &value.prefix);

            map[name].push_back(value);

            if (debug_general)
                printf("   Config: added prefix: %s %s/%d\n", (value.isIPv4 ? "IPv4" : "IPv6"), prefix,
                       value.bits);

            free(prefix_full);          // free strdup
        }
    }
}

/**
 * Perform topic name substitutions based on topic variables
 */
void Config::topicSubstitutions() {
    // not the fastest update, but this is fine since it's only done on startup
    for (topic_vars_map_iter v_it = topic_vars_map.begin(); v_it != topic_vars_map.end(); ++v_it) {
        std::string var = "{";
        var += v_it->first;
        var += "}";

        for (topic_names_map_iter n_it = topic_names_map.begin(); n_it != topic_names_map.end(); ++n_it) {
            boost::replace_all(n_it->second, var, v_it->second);
        }
    }
}

/**
 * print warning message for parsing node
 *
 * \param [in] msg      Warning message
 * \param [in] node     Offending node that caused the warning
 */
void Config::printWarning(const std::string msg, const YAML::Node &node) {
    std::string type;

    switch (node.Type()) {
        case YAML::NodeType::Null:
            type = "Null";
            break;
        case YAML::NodeType::Scalar:
            type = "Scalar";
            break;
        case YAML::NodeType::Sequence:
            type = "Sequence";
            break;
        default:
            type = "Unknown";
            break;
    }
    std::cout << "WARN: " << msg << " : " << type << " = " << node.Scalar() << std::endl ;
}

