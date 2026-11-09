#include "client/ObjectClient.h"

#include <iostream>
#include <string>

int main(int argc, char* argv[])
{
    const bool upload = (argc == 4 || argc == 5) && std::string(argv[1]) == "upload";
    const bool download = argc == 5 && std::string(argv[1]) == "download";
    if ((!upload && !download) || argv[2][0] == '\0') {
        std::cerr << "usage: object_client upload COORDINATOR_ADDRESS FILE [STATE_PATH]\n"
                     "       object_client download COORDINATOR_ADDRESS OBJECT_ID OUTPUT\n";
        return 2;
    }
    std::string error;
    object_storage::ObjectClient client(argv[2]);
    if (upload) {
        std::string objectId;
        if (!client.Upload(argv[3], &objectId, &error, argc == 5 ? argv[4] : "")) {
            std::cerr << "upload failed: " << error << '\n';
            return 1;
        }
        std::cout << objectId << '\n';
        return 0;
    }
    if (!client.Download(argv[3], argv[4], &error)) {
        std::cerr << "download failed: " << error << '\n';
        return 1;
    }
    return 0;
}
