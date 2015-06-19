/// @author Alexander Rykovanov 2013
/// @email rykovanov.as@gmail.com
/// @brief Opc Ua server interface.
/// @license GNU LGPL
///
/// Distributed under the GNU LGPL License
/// (See accompanying file LICENSE or copy at 
/// http://www.gnu.org/licenses/lgpl.html)
///

#pragma once

#include <opc/ua/protocol/channel.h>
#include <opc/ua/services/services.h>


#include <memory>

namespace OpcUa
{
    enum ClientConnectionState
    {
      Disconnected = 0, // Initial state
      Connecting,       // 
      CouldNotConnect,  // Connection attempt failed
      Reconnecting,     // Auto-reconnection attempt started
      Connected,        // Successfully connected
      Reconnected,
      ConnectionClosedByServer,
      CommunicationError,  // Communication error happened. 
      Disconnecting
    };

    // This function will be called whenever connection/disconnection process is started and finished or state is changed.
    // Returned value is used to indicate need on retries (for connection attempt and in case of detecting of communication error):
    // If not 0, it is the time interval, after which connect attempt should be performed.
    typedef std::function<uint32_t(ClientConnectionState state, OpcUa::StatusCode statusCode, const std::string& errorMessage)> ConnectionStatusChangeCallback;

    extern ConnectionStatusChangeCallback defaultCallback;
    struct SecureConnectionParams
    {
      std::string EndpointUrl;
      std::string SecurePolicy;
      std::vector<uint8_t> SenderCertificate;
      std::vector<uint8_t> ReceiverCertificateThumbPrint;
      uint32_t SecureChannelId;

      SecureConnectionParams()
        : SecureChannelId(0)
      {
      }
    };

    /// @brief Create server based on opc ua binary protocol.
    /// @param channel channel which will be used for sending requests data.
    Services::SharedPtr CreateBinaryClient(IOChannel::SharedPtr channel, const SecureConnectionParams& params, bool debug = false, ConnectionStatusChangeCallback callback = defaultCallback);
    Services::SharedPtr CreateBinaryClient(const std::string& endpointUrl, bool debug = false, ConnectionStatusChangeCallback callback = defaultCallback);

} // namespace OpcUa
