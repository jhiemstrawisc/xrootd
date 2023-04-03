
#include "lotman.h"
#include <nlohmann/json.hpp>


using json = nlohmann::json;

class LotUpdate {
public:

    json m_update_JSON;
    bool m_include_subdirs;
    std::string m_name;
    double m_usage;
    int m_depth;
    
    LotUpdate(): m_update_JSON{}, m_include_subdirs{false}, m_name{}, m_usage{}, m_depth{0} {}
    LotUpdate(int depth): m_update_JSON{}, m_include_subdirs{false}, m_name{}, m_usage{}, m_depth{depth} {}
    ~LotUpdate() {
        // Lot usage is updated upon the destruction of the root update object
        if (m_depth == 0) {
            json update_JSON;
            update_JSON.push_back(m_update_JSON);
            char *err_msg;
            // To view the update JSON used by LotMan to perform the update:
            //std::cout << "input JSON: " << std::endl << update_JSON.dump(2) << std::endl;
            lotman_update_lot_usage_by_dir(update_JSON.dump().c_str(), &err_msg);
        }
    }

    void create_update_JSON(std::string name, double usage) {
        m_update_JSON["path"] = name;
        m_update_JSON["includes_subdirs"] = m_include_subdirs;
        m_update_JSON["subdirs"] = json::array();
        m_update_JSON["size_GB"] = usage / (1024.0 * 1024.0 * 1024.0);

    }

    void add_subdir(json subdir) {
        m_update_JSON["subdirs"].push_back(subdir);
    }

    void print_usage() {
        std::cout << m_update_JSON.dump(2) << std::endl;
    }

};

