//#include "common/dbconnector.h"
//#include "common/zmqproducerstatetable.h"
//#include "common/table.h"
#include <iostream>
#include <string>
#include <malloc.h>

//#include "gperftools/malloc_extension.h"

#include <string.h>
#include <stdint.h>
#include <vector>
#include <queue>
#include <list>
#include <unistd.h>
#include <errno.h>
#include <system_error>
#include <fstream>
#include <set>
#include <unistd.h>

//#include "common/dbconnector.h"
//#include "common/redisreply.h"
//#include "common/redispipeline.h"
//#include "common/pubsub.h"

#include "json.hpp"
#include "common/producertable.h"

using json = nlohmann::json;
using namespace std;


int exit_press_space()
{
    std::cout << "Press SPACE and then Enter to exit..." << std::endl;
    std::string input;
    std::getline(std::cin, input);
    std::cout << "Exiting program." << std::endl;
    return 0;
}

void set_mallopt()
{
    std::cout << "Set mallopt" << std::endl;
    if (mallopt(M_MMAP_MAX, 1024) == -1) {
        std::cout << "Set M_MMAP_MAX failed" << std::endl;
    }

    if (mallopt(M_MMAP_THRESHOLD, 0) == -1) {
        std::cout << "Set M_MMAP_THRESHOLD failed" << std::endl;
    }

    if (mallopt(M_TRIM_THRESHOLD, 0) == -1) {
        std::cout << "Set M_TRIM_THRESHOLD failed" << std::endl;
    }
}

void memory_alloc_release()
{
    int count = 1000*1000;
    u_int8_t data = 0xff;
    u_int8_t** dataArray = new u_int8_t*[count];
    for (int idx = 0; idx < count; idx++)
    {
        dataArray[idx] = new u_int8_t[1024];
        memset(dataArray[idx], data, 1024);
    }
    
    json j = json::parse("{}");
    //auto connect = redisConnect("http://127.0.0.1", 6379);
    //redisFree(connect);

    for (int idx = 0; idx < count; idx++)
    {
        // check memory before delete
        for (int pos = 0; pos < 1024; pos++)
        {
            if (dataArray[idx][pos] != data)
            {
                std::cout << "Data broken at: " << idx << "--" << pos << std::endl;
            }
        }

        delete[] dataArray[idx];
    }
    delete[] dataArray;

    malloc_trim(0);
    //MallocExtension::instance()->ReleaseFreeMemory();
}

int main()
{
    //set_mallopt();
    memory_alloc_release();
    return exit_press_space();
}