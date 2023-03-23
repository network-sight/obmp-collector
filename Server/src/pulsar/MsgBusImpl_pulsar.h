
#ifndef MSGBUSIMPL_PULSAR_H_
#define MSGBUSIMPL_PULSAR_H_

#define HASH_SIZE 16

#include "MsgBusInterface.hpp"
#include "Logger.h"
#include <string>
#include <map>
#include <vector>
#include <ctime>

#include <pulsar/Client.h>

#include <thread>
#include "safeQueue.hpp"
#include "PulsarTopicSelector.h"

#include "Config.h"

/**
 * \class   msgBus_pulsar
 *
 * \brief   pulsar message bus implementation
  */
class msgBus_pulsar: public MsgBusInterface {
public:
    #define MSGBUS_WORKING_BUF_SIZE         1800000
    #define MSGBUS_API_VERSION              "1.7"

    /******************************************************************//**
     * \brief This function will initialize and connect to pulsar.
     *
     * \details It is expected that this class will start off with a new connection.
     *
     *  \param [in] logPtr      Pointer to Logger instance
     *  \param [in] cfg         Pointer to the config instance
     *  \param [in] c_hash_id   Collector Hash ID
     ********************************************************************/
    msgBus_pulsar(::Logger *logPtr, Config *cfg, u_char *c_hash_id);
    ~msgBus_pulsar();

    /*
     * abstract methods implemented
     * See MsgBusInterface.hpp for method details
     */
    void update_Collector(struct obj_collector &c_obj, collector_action_code action_code);
    void update_Router(struct obj_router &r_entry, router_action_code code);
    void update_Peer(obj_bgp_peer &peer, obj_peer_up_event *up, obj_peer_down_event *down, peer_action_code code);
    void update_baseAttribute(obj_bgp_peer &peer, obj_path_attr &attr, base_attr_action_code code);
    void update_unicastPrefix(obj_bgp_peer &peer, std::vector<obj_rib> &rib, obj_path_attr *attr, unicast_prefix_action_code code);
    void add_StatReport(obj_bgp_peer &peer, obj_stats_report &stats);

    void update_LsNode(obj_bgp_peer &peer, obj_path_attr &attr, std::list<MsgBusInterface::obj_ls_node> &nodes,
                     ls_action_code code);
    void update_LsLink(obj_bgp_peer &peer, obj_path_attr &attr, std::list<MsgBusInterface::obj_ls_link> &links,
                     ls_action_code code);
    void update_LsPrefix(obj_bgp_peer &peer, obj_path_attr &attr, std::list<MsgBusInterface::obj_ls_prefix> &prefixes,
                      ls_action_code code);
    
    void update_L3Vpn(obj_bgp_peer &peer, std::vector<obj_vpn> &vpn, obj_path_attr *attr, vpn_action_code code);

    void update_eVPN(obj_bgp_peer &peer, std::vector<obj_evpn> &vpn, obj_path_attr *attr, vpn_action_code code);

    void send_bmp_raw(u_char *r_hash, obj_bgp_peer &peer, u_char *data, size_t data_len);

    // Debug methods
    void enableDebug();
    void disableDebug();

private:
    char            *prep_buf;                  ///< Large working buffer for message preparation
    unsigned char   *producer_buf;              ///< Producer message buffer
    bool            debug;                      ///< debug flag to indicate debugging
    ::Logger          *logger;                    ///< Logging class pointer

    std::string     collector_hash;             ///< collector hash string value

    uint64_t        router_seq;                 ///< Router add/del sequence
    uint64_t        collector_seq;              ///< Collector add/del sequence
    uint64_t        peer_seq ;                  ///< Peer add/del sequence
    uint64_t        base_attr_seq;              ///< Base attribute sequence
    uint64_t        unicast_prefix_seq;         ///< Unicast prefix sequence
    uint64_t        bmp_stat_seq;               ///< BMP stats sequence
    uint64_t        ls_node_seq;                ///< LS node sequence
    uint64_t        ls_link_seq;                ///< LS link sequence
    uint64_t        ls_prefix_seq;              ///< LS prefix sequence
    uint64_t        l3vpn_seq;                  ///< l3vpn sequence
    uint64_t        evpn_seq;                   ///< evpn sequence

    Config          *cfg;                       ///< Pointer to config instance


    bool isConnected;                           ///< Indicates if pulsar client is connected or not

    // array of hashes
    std::map<std::string, std::string> peer_list;
    typedef std::map<std::string, std::string>::iterator peer_list_iter;

    std::string router_ip;                      ///< Router IP in printed format
    u_char      router_hash[16];                ///< Router Hash in binary format
    std::string router_group_name;              ///< Router group name - if matched


    std::string  pulsarUrl;

    Client  *client;
    
    PulsarTopicSelector *topicSel;               ///< Pulsar proceduer selector/handler

    /**
     * Connects to pulsar broker
     */
    void connect();

    /**
     * Disconnects from pulsar broker
     */
    void disconnect(int wait_ms=2000);

    /**
     * produce message to pulsar
     *
     * \param [in] topic_var     Topic var to use in PulsarTopicSelector::getTopic()
     * \param [in] msg           message to produce
     * \param [in] msg_size      Length in bytes of the message
     * \param [in] rows          Number of rows in data
     * \param [in] key           Hash key
     * \param [in] peer_group    Peer group name - empty/NULL if not set or used
     * \param [in] peer_asn      Peer ASN
     */
    void produce(const char *topic_var, char *msg, size_t msg_size, int rows,
                 std::string key, const std::string *peer_group, uint32_t);

    /**
    * \brief Method to resolve the IP address to a hostname
    *
    *  \param [in]   name      String name (ip address)
    *  \param [out]  hostname  String reference for hostname
    *
    *  \returns true if error, false if no error
    */
    bool resolveIp(std::string name, std::string &hostname);


};

#endif /* MSGBUSIMPL_PULSAR_H_ */
