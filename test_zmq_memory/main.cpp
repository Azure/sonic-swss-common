#include "common/dbconnector.h"
#include "common/zmqproducerstatetable.h"
#include "common/table.h"
#include <iostream>
#include <string>
#include <malloc.h>

#include "route.pb.h"
#include <chrono>
#include "common/json.h"

long serialize_duration_total = 0;
long deserialize_duration_total = 0;
long json_serialize_duration_total = 0;
long json_deserialize_duration_total = 0;

void test()
{
    std::string serialized;
    auto serialize_start = std::chrono::high_resolution_clock::now();
    for (int idx = 0; idx < 1000; idx++)
    {
        // {"dest":"1.0.0.0/32","switch_id":"oid:0x1000000000001","vr":"oid:0x300000000"}
        contacts::KeyOpFieldsValues bulkRequest;
        bulkRequest.set_key("SAI_OBJECT_TYPE_ROUTE_ENTRY");
        bulkRequest.set_op("SET");
        for (int routes = 0; routes < 1000; routes++)
        {
            contacts::FieldsValues* route = bulkRequest.add_fvs();
            route->set_field("{\"dest\":\"1.0.0.0/32\",\"switch_id\":\"oid:0x1000000000001\",\"vr\":\"oid:0x300000000\"}");
            route->set_value("SAI_ROUTE_ENTRY_ATTR_PACKET_ACTION=SAI_PACKET_ACTION_FORWARD|SAI_ROUTE_ENTRY_ATTR_NEXT_HOP_ID=oid:0x1000000000001");
        }
        bulkRequest.SerializeToString(&serialized);
    }
    auto serialize_duration = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - serialize_start);
    serialize_duration_total+=serialize_duration.count();
    auto deserialize_start = std::chrono::high_resolution_clock::now();
    for (int idx = 0; idx < 1000; idx++)
    {
        contacts::KeyOpFieldsValues bulkRequest;
        bulkRequest.ParseFromString(serialized);
    }
    auto deserialize_duration = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - deserialize_start);
    deserialize_duration_total+=deserialize_duration.count();
    // json:
    std::vector<swss::FieldValueTuple> values;
    auto field = "{\"dest\":\"1.0.0.0/32\",\"switch_id\":\"oid:0x1000000000001\",\"vr\":\"oid:0x300000000\"}";
    auto value = "SAI_ROUTE_ENTRY_ATTR_PACKET_ACTION=SAI_PACKET_ACTION_FORWARD|SAI_ROUTE_ENTRY_ATTR_NEXT_HOP_ID=oid:0x1000000000001";
    for (int routes = 0; routes < 1000; routes++)
    {
        values.emplace_back(field, value);
    }
    auto json_serialize_start = std::chrono::high_resolution_clock::now();
    for (int idx = 0; idx < 1000; idx++)
    {
        serialized = swss::JSon::buildJson(values);
    }
    auto json_serialize_duration = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - json_serialize_start);
    json_serialize_duration_total+=json_serialize_duration.count();
    auto json_deserialize_start = std::chrono::high_resolution_clock::now();
    for (int idx = 0; idx < 1000; idx++)
    {
        std::vector<swss::FieldValueTuple> fv;
        swss::JSon::readJson(serialized, fv);
    }
    auto json_deserialize_duration = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - json_deserialize_start);
    json_deserialize_duration_total+=json_deserialize_duration.count();
}

int main()
{
    serialize_duration_total = 0;
    deserialize_duration_total = 0;
    json_serialize_duration_total = 0;
    json_deserialize_duration_total = 0;
    int test_iteration = 100;
    while(test_iteration > 0)
    {
        test();
        printf("test_iteration: %d\n", test_iteration);
        test_iteration--;
    } 
    printf("Serialize in ms: %ld\n", (serialize_duration_total / 100) / 1000);
    printf("Deserialize in ms: %ld\n", (deserialize_duration_total / 100) / 1000);
    printf("Json serialize in ms: %ld\n", (json_serialize_duration_total / 100) / 1000);
    printf("Json deserialize in ms: %ld\n", (json_deserialize_duration_total / 100) / 1000);
}