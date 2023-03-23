/*
 * Copyright (c) 2013-2015 Cisco Systems, Inc. and others.  All rights reserved.
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html
 *
 */

#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <cstdio>
#include <unistd.h>

#include <iostream>
#include <cstring>
#include <cstdlib>
#include <string>
#include <cerrno>

#include "BMPListener.h"
#include "BMPReader.h"
#include "parseBMP.h"
#include "parseBGP.h"
#include "MsgBusInterface.hpp"
#include "Logger.h"
#include "md5.h"

using namespace std;

/**
 * Class constructor
 *
 *  \param [in] logPtr  Pointer to existing Logger for app logging
 *  \param [in] config  Pointer to the loaded configuration
 *
 */
BMPReader::BMPReader(Logger *logPtr, Config *config) {
    debug = false;

    cfg = config;

    logger = logPtr;

    if (cfg->debug_bmp)
        enableDebug();
    
    hasPrevRIBdumpTime = false;
    maxRIBdumpRate = 0;
}

/**
 * Destructor
 */
BMPReader::~BMPReader() {

}


/**
 * Read messages from BMP stream in a loop
 *
 * \param [in]  run         Reference to bool to indicate if loop should continue or not
 * \param [in]  client      Client information pointer
 * \param [in]  mbus_ptr     The database pointer referencer - DB should be already initialized
 */
void BMPReader::readerThreadLoop(bool &run, BMPListener::ClientInfo *client, MsgBusInterface *mbus_ptr) {
    while (run) {

        try {
            if (not ReadIncomingMsg(client, mbus_ptr))
                break;

        } catch (char const *str) {
            run = false;
            break;
        }
    }
}

/**
 * Read messages from BMP stream
 *
 * BMP routers send BMP/BGP messages, this method reads and parses those.
 *
 * \param [in]  client      Client information pointer
 * \param [in]  mbus_ptr     The database pointer referencer - DB should be already initialized
 *
 * \return true if more to read, false if the connection is done/closed
 *
 * \throw (char const *str) message indicate error
 */
bool BMPReader::ReadIncomingMsg(BMPListener::ClientInfo *client, MsgBusInterface *mbus_ptr) {
    bool rval = true;
    string peer_info_key;

    parseBGP *pBGP;                                 // Pointer to BGP parser

    int read_fd = client->pipe_sock > 0 ? client->pipe_sock : client->c_sock;

    // Data storage structures
    MsgBusInterface::obj_bgp_peer p_entry;

    // Initialize the parser for BMP messages
    parseBMP *pBMP = new parseBMP(logger, &p_entry);    // handler for BMP messages

    if (cfg->debug_bmp) {
        enableDebug();
        pBMP->enableDebug();
    }

    char bmp_type = 0;

    MsgBusInterface::obj_router r_object;
    memcpy(router_hash_id, client->hash_id, sizeof(router_hash_id));    // Cache the router hash ID (hash is generated by BMPListener)
    bzero(&r_object, sizeof(r_object));
    memcpy(r_object.hash_id, router_hash_id, sizeof(r_object.hash_id));

    // Setup the router record table object
    memcpy(r_object.ip_addr, client->c_ip, sizeof(client->c_ip));

    try {
        bmp_type = pBMP->handleMessage(read_fd);

        /*
         * Now that we have parsed the BMP message...
         *  add record to the database
         */

        /* Removed - We no longer add router entries to the DB unless we receive a router entry first
        if (bmp_type != parseBMP::TYPE_INIT_MSG)
            mbus_ptr->update_Router(r_object, mbus_ptr->ROUTER_ACTION_FIRST);              // add the router entry
        */

        // only process the peering info if the message includes it
        if (bmp_type != parseBMP::TYPE_INIT_MSG && bmp_type != parseBMP::TYPE_TERM_MSG) {
            // Update p_entry hash_id now that add_Router updated it.
            memcpy(p_entry.router_hash_id, r_object.hash_id, sizeof(r_object.hash_id));
            peer_info_key =  p_entry.peer_addr;
            peer_info_key += p_entry.peer_rd;

            if (bmp_type != parseBMP::TYPE_PEER_UP)
                mbus_ptr->update_Peer(p_entry, NULL, NULL, mbus_ptr->PEER_ACTION_FIRST);     // add the peer entry

            if (not peer_info_map[peer_info_key].using_2_octet_asn and p_entry.isTwoOctet) {
                peer_info_map[peer_info_key].using_2_octet_asn = true;
            }
        }

        /*
         * At this point we only have the BMP header message, what happens next depends
         *      on the BMP message type.
         */
        switch (bmp_type) {
            case parseBMP::TYPE_PEER_DOWN : { // Peer down type

                MsgBusInterface::obj_peer_down_event down_event = {};

                if (pBMP->parsePeerDownEventHdr(read_fd,down_event)) {
                    pBMP->bufferBMPMessage(read_fd);


                    // Prepare the BGP parser
                    pBGP = new parseBGP(logger, mbus_ptr, &p_entry, (char *)r_object.ip_addr,
                                        &peer_info_map[peer_info_key]);

                    if (cfg->debug_bgp)
                       pBGP->enableDebug();

                    // Check if the reason indicates we have a BGP message that follows
                    switch (down_event.bmp_reason) {
                        case 1 : { // Local system close with BGP notify
                            snprintf(down_event.error_text, sizeof(down_event.error_text),
                                    "Local close by (%s) for peer (%s) : ", r_object.ip_addr,
                                    p_entry.peer_addr);
                            pBGP->handleDownEvent(pBMP->bmp_data, pBMP->bmp_data_len, down_event);
                            break;
                        }
                        case 2 : // Local system close, no bgp notify
                        {
                            // Read two byte code corresponding to the FSM event
                            uint16_t fsm_event = 0 ;
                            memcpy(&fsm_event, pBMP->bmp_data, 2);
                            bgp::SWAP_BYTES(&fsm_event);

                            snprintf(down_event.error_text, sizeof(down_event.error_text),
                                    "Local (%s) closed peer (%s) session: fsm_event=%d, No BGP notify message.",
                                    r_object.ip_addr,p_entry.peer_addr, fsm_event);
                            break;
                        }
                        case 3 : { // remote system close with bgp notify
                            snprintf(down_event.error_text, sizeof(down_event.error_text),
                                    "Remote peer (%s) closed local (%s) session: ", r_object.ip_addr,
                                    p_entry.peer_addr);

                            pBGP->handleDownEvent(pBMP->bmp_data, pBMP->bmp_data_len, down_event);
                            break;
                        }
                    }

                    delete pBGP;            // Free the bgp parser after each use.

                    // Add event to the database
                    if (client->initRec) // Require router init first
                        mbus_ptr->update_Peer(p_entry, NULL, &down_event, mbus_ptr->PEER_ACTION_DOWN);

                } else {
                    LOG_ERR("Error with client socket %d", read_fd);
                    // Make sure to free the resource
                    throw "BMPReader: Unable to read from client socket";
                }
                break;
            }

            case parseBMP::TYPE_PEER_UP : // Peer up type
            {
                MsgBusInterface::obj_peer_up_event up_event = {};

                if (pBMP->parsePeerUpEventHdr(read_fd, up_event)) {
                    LOG_INFO("%s: PEER UP Received, local addr=%s:%hu remote addr=%s:%hu", client->c_ip,
                            up_event.local_ip, up_event.local_port, p_entry.peer_addr, up_event.remote_port);

                    pBMP->bufferBMPMessage(read_fd);

                    // Prepare the BGP parser
                    pBGP = new parseBGP(logger, mbus_ptr, &p_entry, (char *)r_object.ip_addr,
                                        &peer_info_map[peer_info_key]);

                    if (cfg->debug_bgp)
                       pBGP->enableDebug();

                    // Parse the BGP sent/received open messages
                    int read = pBGP->handleUpEvent(pBMP->bmp_data, pBMP->bmp_data_len, &up_event);

                                        // Free the bgp parser
                    delete pBGP;

                    // Read info TLV data
                    if (((int)pBMP->bmp_data_len - read) > 0) {
                        SELF_DEBUG("%s: PEER UP has info data, parsing %d bytes", p_entry.peer_addr, pBMP->bmp_data_len - read);
                        pBMP->parsePeerUpInfo(pBMP->bmp_data + read, (int)pBMP->bmp_data_len - read);
                    }

                    // Add the up event to the DB
                    if (client->initRec) // Require router init first
                        mbus_ptr->update_Peer(p_entry, &up_event, NULL, mbus_ptr->PEER_ACTION_UP);

                } else {
                    LOG_NOTICE("%s: PEER UP Received but failed to parse the BMP header.", client->c_ip);
                }
                break;
            }

            case parseBMP::TYPE_ROUTE_MIRROR: { // Route mirroring type
                pBMP->bufferBMPMessage(read_fd);
                u_char *bufPtr = pBMP->bmp_data;

                parseBMP::route_mirror_tlv mirror_tlv;


                // There could be 2 or more TLVs.
                for (int i = 0; i < pBMP->bmp_data_len; i += BMP_MIRROR_TLV_HDR_LEN) {
                    memcpy(&mirror_tlv, bufPtr, BMP_MIRROR_TLV_HDR_LEN);
                    mirror_tlv.data = NULL;
                    bgp::SWAP_BYTES(&mirror_tlv.len);
                    bgp::SWAP_BYTES(&mirror_tlv.type);

                    bufPtr += BMP_MIRROR_TLV_HDR_LEN;                // Move pointer past the tlv header

                    SELF_DEBUG("%s: route mirror TLV type %hu and length %hu being parsed",
                               p_entry.peer_addr, mirror_tlv.type, mirror_tlv.len);

                    if (mirror_tlv.len <= (i - pBMP->bmp_data_len)) {
                        mirror_tlv.data = bufPtr;

                        if (mirror_tlv.type == 0 /* BGP message */) {
                            /*
                             * Read and parse the the BGP message from the client.
                             *     parseBGP will update pulsar directly
                             */
                            pBGP = new parseBGP(logger, mbus_ptr, &p_entry, (char *)r_object.ip_addr,
                                                &peer_info_map[peer_info_key]);

                            if (cfg->debug_bgp)
                                pBGP->enableDebug();

                            pBGP->handleUpdate(mirror_tlv.data, mirror_tlv.len);
                            delete pBGP;
                        }

                        bufPtr += mirror_tlv.len;
                        i += mirror_tlv.len;

                    } else {
                        SELF_DEBUG("Dropping route mirror message due to length %hu > %d",
                                   mirror_tlv.len, (i - pBMP->bmp_data_len));
                        break;
                    }
                }

                break;
            }

            case parseBMP::TYPE_ROUTE_MON : { // Route monitoring type
                pBMP->bufferBMPMessage(read_fd);

                /*
                 * Read and parse the the BGP message from the client.
                 *     parseBGP will update pulsar directly
                 */
                pBGP = new parseBGP(logger, mbus_ptr, &p_entry, (char *)r_object.ip_addr,
                                    &peer_info_map[peer_info_key]);

                if (cfg->debug_bgp)
                    pBGP->enableDebug();

                pBGP->handleUpdate(pBMP->bmp_data, pBMP->bmp_data_len);
   		
                string str(reinterpret_cast<char*>(client->hash_id), 16);  //storing the client hash in a string
                if(client->initRec && cfg->router_baseline_time.find(str) == cfg->router_baseline_time.end())
                        //check if client has received init message and Baseline time is not already calculated
                {
                    peer_info_map_iter it = peer_info_map.begin();
                    while (it != peer_info_map.end() && it->second.endOfRIB)
                        ++it;

                    if (it == peer_info_map.end() || checkRIBdumpRate(p_entry.timestamp_secs,mbus_ptr->ribSeq)) {  //End-Of-RIBs are received for all peers.
                        timeval now;
                        gettimeofday(&now, NULL);
                        cfg->router_baseline_time[str] = 1.2 * (now.tv_sec - client->startTime.tv_sec);  //20% buffer for baseline time
                    }
                }

                delete pBGP;

                break;
            }

            case parseBMP::TYPE_STATS_REPORT : { // Stats Report
                MsgBusInterface::obj_stats_report stats = {};
                if (! pBMP->handleStatsReport(read_fd, stats))

                    // Add to mysql
                    if (client->initRec) // Require router init first
                        mbus_ptr->add_StatReport(p_entry, stats);

                break;
            }

            case parseBMP::TYPE_INIT_MSG : { // Initiation Message
                client->initRec = true; 		//indicating that init message is received for the router/client.

                LOG_INFO("%s: Init message received with length of %u", client->c_ip, pBMP->getBMPLength());
                pBMP->handleInitMsg(read_fd, r_object);
		
                if(cfg->pat_enabled && r_object.hash_type)
                    hashRouter(client, r_object);

                LOG_INFO("Router ID hashed with hash_type: %d", r_object.hash_type);

                // Update the router entry with the details
                mbus_ptr->update_Router(r_object, mbus_ptr->ROUTER_ACTION_INIT);

                break;
            }

            case parseBMP::TYPE_TERM_MSG : { // Termination Message
                LOG_INFO("%s: Term message received with length of %u", client->c_ip, pBMP->getBMPLength());


                pBMP->handleTermMsg(read_fd, r_object);

                LOG_INFO("Proceeding to disconnect router");
                mbus_ptr->update_Router(r_object, mbus_ptr->ROUTER_ACTION_TERM);
                close(client->c_sock);

                rval = false;                           // Indicate connection is closed
                break;
            }

        }
    } catch (char const *str) {
        // Mark the router as disconnected and update the error to be a local disconnect (no term message received)
        LOG_INFO("%s: Caught: %s", client->c_ip, str);
        disconnect(client, mbus_ptr, parseBMP::TERM_REASON_OPENBMP_CONN_ERR, str);

        delete pBMP;                    // Make sure to free the resource
        throw str;
    }
    
    // Send BMP RAW packet data
    if (client->initRec) // Require router init first
        mbus_ptr->send_bmp_raw(router_hash_id, p_entry, pBMP->bmp_packet, pBMP->bmp_packet_len);

    // Free the bmp parser
    delete pBMP;

    return rval;
}

bool BMPReader::checkRIBdumpRate(uint32_t timeStamp, int ribSeq) {
    int time, currRate;                                  

    if(!hasPrevRIBdumpTime) {
	prevRIBdumpTime = timeStamp;
	hasPrevRIBdumpTime = true;
    }

    else {
	time = timeStamp - prevRIBdumpTime;
	if(time > 0) { 
            currRate = ribSeq / time;
	    maxRIBdumpRate = max(currRate, maxRIBdumpRate);
	    if(currRate < maxRIBdumpRate * 0.15) {
		if (!isBelowThresholdDumpRate)
		    belowThresholdInitTime = timeStamp;
		isBelowThresholdDumpRate = true;
		if (timeStamp - belowThresholdInitTime >= 3)
	    	    return true;
	    }
	    else
		isBelowThresholdDumpRate = false;
	}
	prevRIBdumpTime = timeStamp;
    }
    return false;
}

/**
 * disconnect/close bmp stream
 *
 * Closes the BMP stream and disconnects router as needed
 *
 * \param [in]  client      Client information pointer
 * \param [in]  mbus_ptr     The database pointer referencer - DB should be already initialized
 * \param [in]  reason_code The reason code for closing the stream/feed
 * \param [in]  reason_text String detailing the reason for close
 *
 */
void BMPReader::disconnect(BMPListener::ClientInfo *client, MsgBusInterface *mbus_ptr, int reason_code, char const *reason_text) {

    MsgBusInterface::obj_router r_object;
    bzero(&r_object, sizeof(r_object));
    memcpy(r_object.hash_id, router_hash_id, sizeof(r_object.hash_id));
    memcpy(r_object.ip_addr, client->c_ip, sizeof(client->c_ip));

    r_object.term_reason_code = reason_code;
    if (reason_text != NULL)
        snprintf(r_object.term_reason_text, sizeof(r_object.term_reason_text), "%s", reason_text);

    if (client->initRec)
        mbus_ptr->update_Router(r_object, mbus_ptr->ROUTER_ACTION_TERM);

    close(client->c_sock);
    client->c_sock = 0;
}


/**
 * Generate BMP router HASH
 *
 * \param [in,out] client   Pointer to client info and hash_val used to generate the hash.
 *
 * \return client.hash_id will be updated with the generated hash
 */

void BMPReader::hashRouter(BMPListener::ClientInfo *client,MsgBusInterface::obj_router &r_object ) {
    char *hash_val;
    if(r_object.hash_type==2)
        hash_val=r_object.bgp_id;
    else if(r_object.hash_type==1)
        hash_val=(char *)r_object.name;
    else // assume type 0
        hash_val=(char *)r_object.ip_addr;

    string c_hash_str;
    MsgBusInterface::hash_toStr(cfg->c_hash_id, c_hash_str);

    MD5 hash;
    hash.update((unsigned char *)hash_val, strlen(hash_val));
    hash.update((unsigned char *)c_hash_str.c_str(), c_hash_str.length());
    hash.finalize();

    // Save the hash
    unsigned char *hash_bin = hash.raw_digest();
    memcpy(client->hash_id, hash_bin, 16);
    delete[] hash_bin;
    memcpy(router_hash_id, client->hash_id, sizeof(router_hash_id));
    memcpy(r_object.hash_id, router_hash_id, sizeof(r_object.hash_id));
    LOG_INFO("Router ID hashed with hash_type: %d", r_object.hash_type);
}



/*
 * Enable/Disable debug
 */
void BMPReader::enableDebug() {
    debug = true;
}

void BMPReader::disableDebug() {
    debug = false;
}
