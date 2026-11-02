#pragma once

#include <net/if.h>
#include <string>

namespace swss
{

const size_t VRF_NAME_MAX_LEN = IFNAMSIZ - 1;

inline bool isVrfNameValid(const std::string &vrfName)
{
    if (vrfName.empty() || vrfName.length() >= IFNAMSIZ || vrfName == "." || vrfName == "..")
    {
        return false;
    }

    for (size_t index = 0; index < vrfName.size(); ++index)
    {
        const unsigned char character = static_cast<unsigned char>(vrfName[index]);
        const bool isAlphaNumeric =
            (character >= 'A' && character <= 'Z') ||
            (character >= 'a' && character <= 'z') ||
            (character >= '0' && character <= '9');
        const bool isCommonCharacter =
            isAlphaNumeric || character == '_' || character == '.';

        if (isCommonCharacter || (index > 0 && character == '-'))
        {
            continue;
        }

        return false;
    }

    return true;
}

}
