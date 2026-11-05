#include "common/converter.h"

#include "gtest/gtest.h"

using namespace std;
using namespace swss;

TEST(CONVERTER, positive)
{
    EXPECT_EQ(18446744073709551615LU, to_uint<uint64_t>("18446744073709551615"));
    EXPECT_EQ(11111111111LU, to_uint<uint64_t>("11111111111"));
    EXPECT_EQ(0LU, to_uint<uint64_t>("0"));

    EXPECT_EQ(9223372036854775807L, to_int<int64_t>("9223372036854775807"));
    EXPECT_EQ(11111111111L, to_int<int64_t>("11111111111"));
    EXPECT_EQ(0L, to_int<int64_t>("0"));

    EXPECT_EQ(4294967295U, to_uint<uint32_t>("4294967295"));
    EXPECT_EQ(1111111U, to_uint<uint32_t>("1111111"));
    EXPECT_EQ(0U, to_uint<uint32_t>("0"));

    EXPECT_EQ(2147483647, to_int<int32_t>("2147483647"));
    EXPECT_EQ(1111111, to_int<int32_t>("1111111"));
    EXPECT_EQ(0, to_int<int32_t>("0"));

    EXPECT_EQ(65535U, to_uint<uint16_t>("65535"));
    EXPECT_EQ(1111U, to_uint<uint16_t>("1111"));
    EXPECT_EQ(0U, to_uint<uint16_t>("0"));

    EXPECT_EQ(32767, to_int<int16_t>("32767"));
    EXPECT_EQ(1111, to_int<int16_t>("1111"));
    EXPECT_EQ(0, to_int<int16_t>("0"));

    EXPECT_EQ(255U, to_uint<uint8_t>("255"));
    EXPECT_EQ(111U, to_uint<uint8_t>("111"));
    EXPECT_EQ(0U, to_uint<uint8_t>("0"));

    EXPECT_EQ(127, to_int<int8_t>("127"));
    EXPECT_EQ(111, to_int<int8_t>("111"));
    EXPECT_EQ(0, to_int<int8_t>("0"));
}

TEST(CONVERTER, negative)
{
    string val = "str1";

    EXPECT_THROW(to_uint<uint64_t>(val), invalid_argument);
    EXPECT_THROW(to_uint<uint32_t>(val), invalid_argument);
    EXPECT_THROW(to_uint<uint16_t>(val), invalid_argument);
    EXPECT_THROW(to_uint<uint8_t>(val), invalid_argument);

    EXPECT_THROW(to_int<int64_t>(val), invalid_argument);
    EXPECT_THROW(to_int<int32_t>(val), invalid_argument);
    EXPECT_THROW(to_int<int16_t>(val), invalid_argument);
    EXPECT_THROW(to_int<int8_t>(val), invalid_argument);

    val = "1234asd";

    EXPECT_THROW(to_uint<uint64_t>(val), invalid_argument);
    EXPECT_THROW(to_uint<uint32_t>(val), invalid_argument);
    EXPECT_THROW(to_uint<uint16_t>(val), invalid_argument);
    EXPECT_THROW(to_uint<uint8_t>(val), invalid_argument);

    EXPECT_THROW(to_int<int64_t>(val), invalid_argument);
    EXPECT_THROW(to_int<int32_t>(val), invalid_argument);
    EXPECT_THROW(to_int<int16_t>(val), invalid_argument);
    EXPECT_THROW(to_int<int8_t>(val), invalid_argument);

    val = "-1";

    EXPECT_THROW(to_uint<uint64_t>(val), invalid_argument);
    EXPECT_THROW(to_uint<uint32_t>(val), invalid_argument);
    EXPECT_THROW(to_uint<uint16_t>(val), invalid_argument);
    EXPECT_THROW(to_uint<uint8_t>(val), invalid_argument);

    val = "9223372036854775807";

    EXPECT_THROW(to_uint<uint32_t>(val), invalid_argument);
    EXPECT_THROW(to_uint<uint16_t>(val), invalid_argument);
    EXPECT_THROW(to_uint<uint8_t>(val), invalid_argument);

    EXPECT_THROW(to_int<int32_t>(val), invalid_argument);
    EXPECT_THROW(to_int<int16_t>(val), invalid_argument);
    EXPECT_THROW(to_int<int8_t>(val), invalid_argument);;
}

TEST(CONVERTER, uppercase)
{
    string val = "str1";
    EXPECT_EQ("STR1", to_upper(val));

    val = "STR2";
    EXPECT_EQ("STR2", to_upper(val));

    val = "";
    EXPECT_EQ("", to_upper(val));

    const string c_val = "AbCd123$%^";
    EXPECT_EQ("ABCD123$%^", to_upper(c_val));
}
