/// @author Alexander Rykovanov 2012
/// @email rykovanov.as@gmail.com
/// @brief Opc binary cnnection channel.
/// @license GNU LGPL
///
/// Distributed under the GNU LGPL License
/// (See accompanying file LICENSE or copy at 
/// http://www.gnu.org/licenses/lgpl.html)
///

#include <opc/ua/socket_channel.h>
#include <opc/ua/errors.h>


#include <errno.h>
#include <iostream>

#include <stdexcept>
#include <string.h>

#include <sys/types.h>


#ifdef _WIN32
#include <WinSock2.h>
#else
#include <arpa/inet.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>
#endif


OpcUa::SocketChannel::SocketChannel(int sock)
  : Socket(sock)
{
  int flag = 1;
  setsockopt(sock, IPPROTO_TCP, TCP_NODELAY, (char *) &flag, sizeof(int));
  if (Socket < 0)
  {
    THROW_ERROR(CannotCreateChannelOnInvalidSocket);
  }
}

OpcUa::SocketChannel::~SocketChannel()
{
  Stop();
}

void OpcUa::SocketChannel::Stop(StopType st)
{
  if (Socket != ~0)
  {
    int error = shutdown(Socket, (int)st);
    if (error < 0)
    {
      std::cerr << "Failed to stop socket connection for type " << (int)st << ", error: " << strerror(errno) << std::endl;
    }
  }
}

void OpcUa::SocketChannel::Stop()
{
  Stop(StopType::StopBoth);
}
void OpcUa::SocketChannel::Close()
{
  if (Socket != ~0)
  {
    int error = closesocket(Socket);
    if (error != 0)
    {
      std::cerr << "Failed to close socket connection. " << strerror(errno) << std::endl;
    }
    else
    {
      std::cout << "SocketChannel| Stop: closed socket connection. " << std::endl;
    }
    Socket = ~0;
  }
}

std::size_t OpcUa::SocketChannel::Receive(char* data, std::size_t size)
{
  int received = recv(Socket, data, size, MSG_WAITALL);
  if (received < 0)
  {
    THROW_OS_ERROR("Failed to receive data from host.");
  }
  if (received == 0)
  {
    THROW_OS_ERROR("Connection was closed by host.");
  }
  if (size != received)
  {
    int i = 0;
    i++;
  } 
  return (std::size_t)received;
}

void OpcUa::SocketChannel::Send(const char* message, std::size_t size)
{
  int sent = send(Socket, message, size, 0);
  if (sent != (int)size)
  {
    THROW_OS_ERROR("unable to send data to the host. ");
  }
}
