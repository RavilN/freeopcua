/// @author Alexander Rykovanov 2012
/// @email rykovanov.as@gmail.com
/// @brief Opc binary cnnection channel.
/// @license GNU LGPL
///
/// Distributed under the GNU LGPL License
/// (See accompanying file LICENSE or copy at 
/// http://www.gnu.org/licenses/lgpl.html)
///

#include <opc/ua/client/remote_connection.h>
#include <opc/ua/errors.h>
#include <opc/ua/socket_channel.h>


#include <errno.h>
#include <iostream>
#include <stdexcept>
#include <string.h>
#include <sys/types.h>

#ifdef _WIN32
  #include <winsock2.h>
  #include <ws2tcpip.h>
#else
  #include <arpa/inet.h>
  #include <netdb.h>
  #include <netinet/in.h>
  #include <sys/socket.h>
#endif

namespace
{

  unsigned long GetIPAddress(const std::string& hostName)
  {
    // TODO Use getaddrinfo
#ifdef _WIN32

    unsigned long resolvedAddress = 0;
    WSADATA wsaData;
    int iResult;
    INT iRetval;

    DWORD dwRetval;

    int i = 1;

    struct addrinfo *result = NULL;
    struct addrinfo *ptr = NULL;
    struct addrinfo hints;

    struct sockaddr_in  *sockaddr_ipv4;
    //    struct sockaddr_in6 *sockaddr_ipv6;
    LPSOCKADDR sockaddr_ip;

    char ipstringbuffer[46];
    DWORD ipbufferlength = 46;

    //// Validate the parameters
    //if (argc != 3) {
    //  printf("usage: %s <hostname> <servicename>\n", argv[0]);
    //  printf("getaddrinfo provides protocol-independent translation\n");
    //  printf("   from an ANSI host name to an IP address\n");
    //  printf("%s example usage\n", argv[0]);
    //  printf("   %s www.contoso.com 0\n", argv[0]);
    //  return 1;
    //}

    //--------------------------------
    // Setup the hints address info structure
    // which is passed to the getaddrinfo() function
    ZeroMemory(&hints, sizeof(hints));
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_protocol = IPPROTO_TCP;

    dwRetval = getaddrinfo(hostName.c_str(), "0", &hints, &result);
    if (dwRetval != 0) {
      throw std::string("getaddrinfo failed with error ") + std::to_string(dwRetval);
    }

    // Retrieve each address and print out the hex bytes
    for (ptr = result; ptr != NULL && resolvedAddress == 0; ptr = ptr->ai_next) {
      switch (ptr->ai_family) {
      case AF_UNSPEC:
        break;
      case AF_INET:
        {
          printf("AF_INET (IPv4)\n");
          sockaddr_ipv4 = (struct sockaddr_in *) ptr->ai_addr;
          IN_ADDR addr = sockaddr_ipv4->sin_addr;
          resolvedAddress = addr.S_un.S_addr;
          std::string resolvedAddressAsString = inet_ntoa(sockaddr_ipv4->sin_addr);
        }
        break;
      case AF_INET6:
        // the InetNtop function is available on Windows Vista and later
        // sockaddr_ipv6 = (struct sockaddr_in6 *) ptr->ai_addr;
        // printf("\tIPv6 address %s\n",
        //    InetNtop(AF_INET6, &sockaddr_ipv6->sin6_addr, ipstringbuffer, 46) );

        // We use WSAAddressToString since it is supported on Windows XP and later
        sockaddr_ip = (LPSOCKADDR)ptr->ai_addr;
        // The buffer length is changed by each call to WSAAddresstoString
        // So we need to set it for each iteration through the loop for safety
        ipbufferlength = 46;
        iRetval = WSAAddressToString(sockaddr_ip, (DWORD)ptr->ai_addrlen, NULL,
          ipstringbuffer, &ipbufferlength);
        if (iRetval)
          printf("WSAAddressToString failed with %u\n", WSAGetLastError());
        else
          printf("\tIPv6 address %s\n", ipstringbuffer);
        break;
      case AF_NETBIOS:
        printf("AF_NETBIOS (NetBIOS)\n");
        break;
      default:
        printf("Other %ld\n", ptr->ai_family);
        break;
      }
      switch (ptr->ai_socktype) {
      case 0:
        printf("Unspecified\n");
        break;
      case SOCK_STREAM:
        printf("SOCK_STREAM (stream)\n");
        break;
      case SOCK_DGRAM:
        printf("SOCK_DGRAM (datagram) \n");
        break;
      case SOCK_RAW:
        printf("SOCK_RAW (raw) \n");
        break;
      case SOCK_RDM:
        printf("SOCK_RDM (reliable message datagram)\n");
        break;
      case SOCK_SEQPACKET:
        printf("SOCK_SEQPACKET (pseudo-stream packet)\n");
        break;
      default:
        printf("Other %ld\n", ptr->ai_socktype);
        break;
      }
      printf("\tProtocol: ");
      switch (ptr->ai_protocol) {
      case 0:
        printf("Unspecified\n");
        break;
      case IPPROTO_TCP:
        printf("IPPROTO_TCP (TCP)\n");
        break;
      case IPPROTO_UDP:
        printf("IPPROTO_UDP (UDP) \n");
        break;
      default:
        printf("Other %ld\n", ptr->ai_protocol);
        break;
      }
      printf("\tLength of this sockaddr: %d\n", ptr->ai_addrlen);
      printf("\tCanonical name: %s\n", ptr->ai_canonname);
    }

    freeaddrinfo(result);
    return resolvedAddress;

#else
    hostent* host = gethostbyname(hostName.c_str());
    if (!host)
    {
      THROW_OS_ERROR("Unable to to resolve host '" + hostName + "'.");
    }
    return *(unsigned long*)host->h_addr_list[0];
#endif
  }

  int ConnectToRemoteHost(const std::string& host, unsigned short port)
  {
#ifdef _WIN32
    int sock = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
#else
    int sock = socket(AF_INET, SOCK_STREAM, 0);
#endif

    if (sock < 0)
    {
      THROW_OS_ERROR("Unable to create socket for connecting to the host '" + host + ".");
    }

    int connectionTimeout = 10000;
    int longestReceiveTimeout = 120000; //Should be something like server state read interval + read request timeout 

    int setSocketOptionError = setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, (char*)&connectionTimeout, sizeof(int));

    sockaddr_in addr = {0};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    addr.sin_addr.s_addr = GetIPAddress(host);
    int error = connect(sock, (sockaddr*)& addr, sizeof(addr));
    if (error < 0)
    {
      int socketError = WSAGetLastError();
      int closeResult = closesocket(sock);
      int closeError = 0;
      if (closeResult < 0)
      {
        closeError = WSAGetLastError();
      }
      sock = ~0;
      THROW_OS_ERROR(std::string("Unable connect to host '") + host + std::string("'."));
    }

    setSocketOptionError = setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, (char*)&longestReceiveTimeout, sizeof(int));

    return sock;
  }

  class BinaryConnection : public OpcUa::RemoteConnection
  {
  public:
    BinaryConnection(int sock, const std::string& host, unsigned short port)
      : HostName(host)
      , Port(port)
      , Channel(sock)
    {
    }

    virtual ~BinaryConnection()
    {
    }

    virtual std::size_t Receive(char* data, std::size_t size)
    {
      return Channel.Receive(data, size);
    }

    virtual void Send(const char* message, std::size_t size)
    {
      return Channel.Send(message, size);
    }

    virtual void Close()
    {
      Channel.Close();
    }

    virtual void Stop()
    {
      Stop(StopType::StopBoth);
    }
    virtual void Stop(StopType st)
    {
      Channel.Stop(st);
    }
    virtual std::string GetHost() const
    {
      return HostName;
    }

    virtual unsigned GetPort() const
    {
      return Port;
    }

  private:
    const std::string HostName;
    const unsigned Port;
    OpcUa::SocketChannel Channel;
  };

}

std::unique_ptr<OpcUa::RemoteConnection> OpcUa::Connect(const std::string& host, unsigned port)
{
  const int sock = ConnectToRemoteHost(host, port);
  return std::unique_ptr<RemoteConnection>(new BinaryConnection(sock, host, port));
}

