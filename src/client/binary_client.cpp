/// @author Alexander Rykovanov 2012
/// @email rykovanov.as@gmail.com
/// @brief Remote server implementation.
/// @license GNU LGPL
///
/// Distributed under the GNU LGPL License
/// (See accompanying file LICENSE or copy at 
/// http://www.gnu.org/licenses/lgpl.html)
///

#include <opc/ua/protocol/utils.h>
#include <opc/ua/client/binary_client.h>
#include <opc/ua/client/remote_connection.h>

#include <opc/common/uri_facade.h>
#include <opc/ua/protocol/binary/stream.h>
#include <opc/ua/protocol/channel.h>
#include <opc/ua/protocol/secure_channel.h>
#include <opc/ua/protocol/session.h>
#include <opc/ua/protocol/string_utils.h>
#include <opc/ua/services/services.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <queue>
#include <thread>
#include <iostream>

#include "opc/ua/client/async_ua_client.h"

namespace
{
  using namespace OpcUa;
  using namespace OpcUa::Binary;

  typedef std::map<IntegerId, std::function<void (PublishResult)>> SubscriptionCallbackMap;

  class BufferInputChannel : public OpcUa::InputChannel
  {
  public:
     BufferInputChannel(const std::vector<char>& buffer)
       : Buffer(buffer)
       , Pos(0)
     {
       Reset();
     }

     virtual std::size_t Receive(char* data, std::size_t size)
     {
       if (Pos >= Buffer.size())
       {
         return 0;
       }

       size = std::min(size, Buffer.size() - Pos);
       std::vector<char>::const_iterator begin = Buffer.begin() + Pos;
       std::vector<char>::const_iterator end = begin + size;
       std::copy(begin, end, data);
       Pos += size;
       return size;
     }

     void Reset()
     {
       Pos = 0;
     }

     virtual void Stop()
     {
     }

  private:
    const std::vector<char>& Buffer;
    std::size_t Pos;
  };

  template <typename T>
  class RequestCallback
  {
  public:
    RequestCallback()
    {
      dataReceived = false;
    }
    ~RequestCallback()
    {
      std::lock_guard<std::mutex> lock(m);
    }

    void OnData(std::vector<char> data, ResponseHeader h)
    {
      {
        std::lock_guard<std::mutex> lock(m);
        Data = std::move(data);
        this->header = std::move(h);
        dataReceived = true;
      }
      doneEvent.notify_all();
    }

    T WaitForData(std::chrono::milliseconds msec)
    {
      T result;
      if (result.TypeId == OpcUa::CLOSE_SECURE_CHANNEL_REQUEST)
      {
        return result;
      }
      if (msec.count() == 0)
      {
        msec = std::chrono::milliseconds(0x7FFFFFFF); //TODO - put proper max value
      }
      {
        std::unique_lock<std::mutex> lock(m);
        doneEvent.wait_for(lock, msec, [=] {return dataReceived; }); // This wait_for call releases the lock and waits for notification
        // When notification is received, the lock is acquired again, so now it is safe to access data:

        result.Header = std::move(this->header);
        if ((((uint32_t)header.ServiceResult) & 0xC0000000) == 0)
        {
          try
          {
            BufferInputChannel bufferInput(Data);
            IStreamBinary in(bufferInput);
            in >> result;
          }
          catch (std::exception ex)
          {
            result.Header.ServiceResult = OpcUa::StatusCode::BadDecodingError;
          }
        }
      }
      return result;
    }

  private:
    std::vector<char> Data;
	  ResponseHeader	  header;
    std::mutex m;
    std::condition_variable doneEvent;
    bool dataReceived;
  };

  class CallbackThread
  {
    public:
      CallbackThread(bool debug=false) : Debug(debug), StopRequest(false)
      {

      }

      void post(std::function<void()> callback)
      {
        if (Debug)  { std::cout << "binary_client| CallbackThread :  start post" << std::endl; }
        {
          std::lock_guard<std::mutex> lock(Mutex);
          Queue.push(callback);
        }
        Condition.notify_one();
        if (Debug)  { std::cout << "binary_client| CallbackThread :  end post" << std::endl; }
      }

      void Run()
      {
        bool exitFlag = false;
        while (!exitFlag)
        {
          if (Debug)  { std::cout << "binary_client| CallbackThread : waiting for next post" << std::endl; }
          std::queue<std::function<void()>> queueCopy;
          {
            std::unique_lock<std::mutex> lock(Mutex);
            Condition.wait(lock, [&]() { return (StopRequest == true) || (!Queue.empty()); });

            
            // At this point the Mutex is acquired, so variables can be safely accessed:
            if (StopRequest)
            {
              if (Debug)  { std::cout << "binary_client| CallbackThread : exited." << std::endl; }
              exitFlag = true;
            }
            else
            {
              queueCopy = Queue;
              while (!Queue.empty())
              {
                Queue.pop();
              }
            }
          }
          Condition.notify_one();
         
          if (Debug) 
          {
            std::cout << "binary_client| CallbackThread : Processing  entries from queue, size is  " << queueCopy.size() << std::endl;
          }
          while (!queueCopy.empty()) 
          {
              std::function<void()> callback = queueCopy.front();
              queueCopy.pop();
              if (Debug)  { std::cout << "binary_client| CallbackThread : now calling callback." << std::endl; }
              callback();
              if (Debug)  { std::cout << "binary_client| CallbackThread : callback has been called." << std::endl; }
          }
        }
      }

      void Stop()
      {
        if (Debug)  { std::cout << "binary_client| CallbackThread : stopping." << std::endl; }
        {
          std::lock_guard<std::mutex> lock(Mutex);
          StopRequest = true;
        }
        Condition.notify_all();
      }

    private:
      bool Debug = false;
      std::mutex Mutex;
      std::condition_variable Condition;
      bool StopRequest;
      std::queue<std::function<void()>> Queue;
  };

  class BinaryClient
    : public Services
    , public AttributeServices
    , public EndpointServices
    , public MethodServices
    , public NodeManagementServices
    , public SubscriptionServices
    , public ViewServices
    , public OpcUa::AsyncUaClient
    , public std::enable_shared_from_this < BinaryClient >
  {
  private:
    typedef std::function<void(std::vector<char>, ResponseHeader)> ResponseCallback;
    typedef std::map<uint32_t, std::pair<std::chrono::system_clock::time_point, ResponseCallback>> CallbackMap;
    OpenSecureChannelParameters secureChannelParams;

  public:
    BinaryClient(std::shared_ptr<IOChannel> channel, const SecureConnectionParams& params, bool debug, ConnectionStatusChangeCallback callback)
      : Channel(channel)
      , Stream(channel)
      , Params(params)
      , SequenceNumber(1)
      , RequestNumber(1)
      , RequestHandle(0)
      , Debug(debug)
      , CallbackService(debug)
    {
      statusChangeCallback = callback;
      ConnectionState = ClientConnectionState::Disconnected;

      HelloServer(params);
      //Initialize the worker thread for subscriptions
      callback_thread = std::thread([&](){ CallbackService.Run(); });
      requestQueueCleanerThread = std::thread([&](){
        while (!Finished)
        {
          //Remove timed out requests from the queue and call callback functions to let higher level application know:
          Mutex.lock();
          ResponseHeader header;
          header.ServiceResult = OpcUa::StatusCode::BadTimeout;
          for (CallbackMap::iterator callbackIt = Callbacks.begin(); callbackIt != Callbacks.end(); )
          {
            std::chrono::steady_clock::time_point requestTimeoutTime = callbackIt->second.first;

            int32_t timeOverdue = 0;
            if (std::chrono::steady_clock::now() > requestTimeoutTime)
            {
              timeOverdue = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - requestTimeoutTime).count();
            }
            
            if (timeOverdue > 100)
            {
              header.RequestHandle = callbackIt->first;
              try
              {
                std::vector<char> emptyContent;
                if (Debug)
                {
                  std::cout << "Request with id = " << header.RequestHandle << " timed out - calling back ... " << std::endl;
                }
                callbackIt->second.second(emptyContent, header);
              }
              catch (std::exception ex)
              {
                auto message = ex.what();
                if (Debug) std::cout << "Exception caught on callback call: " << ex.what() << std::endl;
              }
              try
              {
                callbackIt = Callbacks.erase(callbackIt);
              }
              catch (std::exception ex)
              {
                auto message = ex.what();
                if (Debug) std::cout << "Exception caught on attempt to erase RequestCallback: " << ex.what() << std::endl;
              }
              if (Debug)
              {
                std::cout << "Timed out request with id = " << header.RequestHandle << " removed from callbacks queue " << std::endl;
              }
            }
            else
            {
              callbackIt++;
            }
          }
          Mutex.unlock();
          std::this_thread::sleep_for(std::chrono::microseconds(1000));
        }
      });
      ReceiveThread = std::move(std::thread([=]()
      {
        while (!Finished && Receive());
      }));
    }

    void StopReceiving()
    {
      ConnectionState = ClientConnectionState::Disconnecting;
      Finished = true;

      if (Debug) std::cout << "binary_client| Stopping callback thread." << std::endl;
      CallbackService.Stop();

      if (Debug) std::cout << "binary_client| Stopping communication channel for read." << std::endl;
      Channel->Stop(BreakableChannel::StopReceive);
      if (Debug) std::cout << "binary_client| Communication channel stopped for read." << std::endl;
    }

    void CloseConnection()
    {
      try
      {
        Channel->Close();
      }
      catch (std::exception ex)
      {
        if (Debug) std::cout << "binary_client| Error closing socket: " << ex.what() << std::endl;
      }
    }
    ~BinaryClient()
    {
      StopReceiving();
      CloseConnection();
      if (Debug) std::cout << "binary_client| Joining callback service thread." << std::endl;
      callback_thread.join();

      if (Debug) std::cout << "binary_client| Joining receive thread." << std::endl;
      ReceiveThread.join();
      if (Debug) std::cout << "binary_client| Receive tread stopped." << std::endl;
      
      requestQueueCleanerThread.join();

      if (Debug) std::cout << "binary_client| Cleaning callbacks queue" << std::endl;
      
      Mutex.lock();
      ResponseHeader header;
      header.ServiceResult = OpcUa::StatusCode::BadSessionClosed;
      for (CallbackMap::iterator callbackIt = Callbacks.begin(); callbackIt != Callbacks.end(); callbackIt++)
      {
        header.RequestHandle = callbackIt->first;
        callbackIt->second.second(std::vector<char>(), header);
      }
      Callbacks.clear();
      Mutex.unlock();
      if (Debug) std::cout << "binary_client| deleted." << std::endl;
    }


    ////////////////////////////////////////////////////////////////
    /// Session Services
    ////////////////////////////////////////////////////////////////
    virtual CreateSessionResponse CreateSession(const RemoteSessionParameters& parameters)
    {
      ConnectionState = ClientConnectionState::Connecting;
      //statusChangeCallback(ConnectionState, OpcUa::StatusCode::Good, "");
      if (Debug)  { std::cout << "binary_client| CreateSession -->" << std::endl; }
      CreateSessionRequest request;
      CreateRequestHeader(request.Header);

      request.Parameters.ClientDescription.URI = parameters.ClientDescription.URI;
      request.Parameters.ClientDescription.ProductURI = parameters.ClientDescription.ProductURI;
      request.Parameters.ClientDescription.Name = parameters.ClientDescription.Name;
      request.Parameters.ClientDescription.Type = parameters.ClientDescription.Type;
      request.Parameters.ClientDescription.GatewayServerURI = parameters.ClientDescription.GatewayServerURI;
      request.Parameters.ClientDescription.DiscoveryProfileURI = parameters.ClientDescription.DiscoveryProfileURI;
      request.Parameters.ClientDescription.DiscoveryURLs = parameters.ClientDescription.DiscoveryURLs;

      request.Parameters.ServerURI = parameters.ServerURI;
      request.Parameters.EndpointURL = parameters.EndpointURL; // TODO make just endpoint.URL;
      request.Parameters.SessionName = parameters.SessionName;
      request.Parameters.ClientNonce = std::vector<uint8_t>(32,0);
      request.Parameters.ClientCertificate = parameters.ClientCertificate;
      request.Parameters.RequestedSessionTimeout = parameters.Timeout;
      request.Parameters.MaxResponseMessageSize = parameters.MaxResponseMessageSize;
      CreateSessionResponse response = Send<CreateSessionResponse>(request);
      AuthenticationToken = response.Session.AuthenticationToken;
      if (Debug)  { std::cout << "binary_client| CreateSession <--" << std::endl; }
      return response;
    }

    ActivateSessionResponse ActivateSession(const UpdatedSessionParameters &session_parameters) override
    {
      ClientConnectionState previousState = ConnectionState;
      if (Debug)  { std::cout << "binary_client| ActivateSession -->" << std::endl; }
      ActivateSessionRequest request;
      request.Parameters = session_parameters;
      request.Parameters.LocaleIds.push_back("en");
      ActivateSessionResponse response = Send<ActivateSessionResponse>(request);
      if (Debug)  { std::cout << "binary_client| ActivateSession <--" << std::endl; }

      if (previousState == ClientConnectionState::Reconnecting)
      {
        ConnectionState = ClientConnectionState::Reconnected;
      }
      else
      { 
        ConnectionState = ClientConnectionState::Connected;
      }
      //statusChangeCallback(ConnectionState, response.Header.ServiceResult, "");
      return response;
    }

    virtual CloseSessionResponse CloseSession()
    {
      ConnectionState = ClientConnectionState::Disconnecting;
      //statusChangeCallback(ConnectionState, StatusCode::Good, "");
      if (Debug)  { std::cout << "binary_client| CloseSession -->" << std::endl; }
      CloseSessionRequest request;
      CloseSessionResponse response;
      response = Send<CloseSessionResponse>(request);
      if (Debug)  { std::cout << "binary_client| CloseSession <--" << " status code " << (uint32_t) response.Header.ServiceResult << std::endl; }
      ConnectionState = ClientConnectionState::Disconnected;
      //statusChangeCallback(ConnectionState, StatusCode::Good, "");
      return response;
    }

    ////////////////////////////////////////////////////////////////
    /// Attribute Services
    ////////////////////////////////////////////////////////////////
    virtual std::shared_ptr<AttributeServices> Attributes() override
    {
      return shared_from_this();
    }

  public:
    virtual std::vector<DataValue> Read(const ReadParameters& params) const
    {
      if (Debug)  {
        std::cout << "binary_client| Read -->" << std::endl;
        for ( ReadValueId attr : params.AttributesToRead )
        {
          std::cout << attr.NodeId << "  " << (uint32_t)attr.AttributeId;
        }
        std::cout << std::endl;
      }
      ReadRequest request;
      request.Parameters = params;
      const ReadResponse response = Send<ReadResponse>(request);
      if (Debug)  { std::cout << "binary_client| Read <--" << std::endl; }
      return response.Results;
    }

    virtual std::vector<OpcUa::StatusCode> Write(const std::vector<WriteValue>& values)
    {
      if (Debug)  { std::cout << "binary_client| Write -->" << std::endl; }
      WriteRequest request;
      request.Parameters.NodesToWrite = values;
      const WriteResponse response = Send<WriteResponse>(request);
      if (Debug)  { std::cout << "binary_client| Write <--" << std::endl; }
      return response.Results;
    }

    ////////////////////////////////////////////////////////////////
    /// Endpoint Services
    ////////////////////////////////////////////////////////////////
    virtual std::shared_ptr<EndpointServices> Endpoints() override
    {
      return shared_from_this();
    }

    virtual std::vector<ApplicationDescription> FindServers(const FindServersParameters& params) const
    {
      if (Debug)  { std::cout << "binary_client| FindServers -->" << std::endl; }
      OpcUa::FindServersRequest request;
      request.Parameters = params;
      FindServersResponse response = Send<FindServersResponse>(request);
      if (Debug)  { std::cout << "binary_client| FindServers <--" << std::endl; }
      return response.Data.Descriptions;
    }

    virtual std::vector<EndpointDescription> GetEndpoints(const EndpointsFilter& filter) const
    {
      if (Debug)  { std::cout << "binary_client| GetEndpoints -->" << std::endl; }
      OpcUa::GetEndpointsRequest request;
      CreateRequestHeader(request.Header);
      request.Filter.EndpointURL = filter.EndpointURL;
      request.Filter.LocaleIds = filter.LocaleIds;
      request.Filter.ProfileUries = filter.ProfileUries;
      const GetEndpointsResponse response = Send<GetEndpointsResponse>(request);
      if (Debug)  { std::cout << "binary_client| GetEndpoints <--" << std::endl; }
      return response.Endpoints;
    }

    virtual void RegisterServer(const ServerParameters& parameters)
    {
    }

    ////////////////////////////////////////////////////////////////
    /// Method Services
    ////////////////////////////////////////////////////////////////
    virtual std::shared_ptr<MethodServices> Method() override
    {
      return shared_from_this();
    }

    virtual std::vector<CallMethodResult> Call(const std::vector<CallMethodRequest>& methodsToCall)
    {
      if (Debug) {std::cout << "binary_clinent | Call -->" << std::endl;}
      CallRequest request;
      request.MethodsToCall = methodsToCall;
      const CallResponse response = Send<CallResponse>(request);
      if (Debug) {std::cout << "binary_clinent | Call <--" << std::endl;}
      return response.Results;
    }

    ////////////////////////////////////////////////////////////////
    /// Node management Services
    ////////////////////////////////////////////////////////////////
    virtual std::shared_ptr<NodeManagementServices> NodeManagement() override
    {
      return shared_from_this();
    }

    virtual std::vector<AddNodesResult> AddNodes(const std::vector<AddNodesItem>& items)
    {
      if (Debug)  { std::cout << "binary_client| AddNodes -->" << std::endl; }
      AddNodesRequest request;
      request.Parameters.NodesToAdd = items;
      const AddNodesResponse response = Send<AddNodesResponse>(request);
      if (Debug)  { std::cout << "binary_client| AddNodes <--" << std::endl; }
      return response.results;
    }

    virtual std::vector<StatusCode> AddReferences(const std::vector<AddReferencesItem>& items)
    {
      if (Debug)  { std::cout << "binary_client| AddReferences -->" << std::endl; }
      AddReferencesRequest request;
      request.Parameters.ReferencesToAdd = items;
      const AddReferencesResponse response = Send<AddReferencesResponse>(request);
      if (Debug)  { std::cout << "binary_client| AddReferences <--" << std::endl; }
      return response.Results;
    }

    ////////////////////////////////////////////////////////////////
    /// Subscriptions Services
    ////////////////////////////////////////////////////////////////
    virtual std::shared_ptr<SubscriptionServices> Subscriptions() override
    {
      return shared_from_this();
    }

    virtual SubscriptionData CreateSubscription(const CreateSubscriptionRequest& request, std::function<void (PublishResult)> callback)
    {
      if (Debug)  { std::cout << "binary_client| CreateSubscription -->" << std::endl; }
      const CreateSubscriptionResponse response = Send<CreateSubscriptionResponse>(request);
      if (Debug) std::cout << "BinaryClient | got CreateSubscriptionResponse" << std::endl;
      PublishCallbacks[response.Data.Id] = callback;// TODO Pass callback to the Publish method.
      if (Debug)  { std::cout << "binary_client| CreateSubscription <--" << std::endl; }
      return response.Data;
    }

    virtual std::vector<StatusCode> DeleteSubscriptions(const std::vector<IntegerId>& subscriptions)
    {
      if (Debug)  { std::cout << "binary_client| DeleteSubscriptions -->" << std::endl; }
      DeleteSubscriptionRequest request;
      request.SubscriptionsIds = subscriptions;
      const DeleteSubscriptionResponse response = Send<DeleteSubscriptionResponse>(request);
      if (Debug)  { std::cout << "binary_client| DeleteSubscriptions <--" << std::endl; }
      return response.Results;
    }

    virtual MonitoredItemsData CreateMonitoredItems(const MonitoredItemsParameters& parameters)
    {
      if (Debug)  { std::cout << "binary_client| CreateMonitoredItems -->" << std::endl; }
      CreateMonitoredItemsRequest request;
      request.Parameters = parameters;
      const CreateMonitoredItemsResponse response = Send<CreateMonitoredItemsResponse>(request);
      if (Debug)  { std::cout << "binary_client| CreateMonitoredItems <--" << std::endl; }
      return response.Data;
    }

    virtual std::vector<StatusCode> DeleteMonitoredItems(const DeleteMonitoredItemsParameters& params)
    {
      if (Debug)  { std::cout << "binary_client| DeleteMonitoredItems -->" << std::endl; }
      DeleteMonitoredItemsRequest request;
      request.Parameters = params;
      const DeleteMonitoredItemsResponse response = Send<DeleteMonitoredItemsResponse>(request);
      if (Debug)  { std::cout << "binary_client| DeleteMonitoredItems <--" << std::endl; }
      return response.Results;
    }

    virtual void Publish(const PublishRequest& originalrequest)
    {
      if (Debug) {std::cout << "binary_client| Publish -->" << "request with " << originalrequest.Parameters.Acknowledgements.size() << " acks" << std::endl;}
      PublishRequest request(originalrequest);
      CreateRequestHeader(request.Header);
      request.Header.Timeout = 0; //We do not want the request to timeout!

      ResponseCallback responseCallback = [this](std::vector<char> buffer, ResponseHeader h)
      {
        if (Debug) 
        {
          std::cout << "BinaryClient | Got Publish Response, from server " << std::endl;
        }
        PublishResponse response;
        if (h.ServiceResult != OpcUa::StatusCode::Good)
        {
          response.Header = std::move(h);
        }
        else
        {
          BufferInputChannel bufferInput(buffer);
          IStreamBinary in(bufferInput);
          in >> response;
        }
        
        CallbackService.post([this, response]() 
        { 
			    if (response.Header.ServiceResult == OpcUa::StatusCode::Good)
			    {
				    if (Debug) { std::cout << "BinaryClient | Calling callback for Subscription " << response.Result.SubscriptionId << std::endl; }
				    SubscriptionCallbackMap::const_iterator callbackIt = this->PublishCallbacks.find(response.Result.SubscriptionId);
				    if (callbackIt == this->PublishCallbacks.end())
				    {
					    std::cout << "BinaryClient | Error Unknown SubscriptionId " << response.Result.SubscriptionId << std::endl;
				    }
				    else
				    {
					    try { //calling client code, better put it under try/catch otherwise we crash entire client
						    callbackIt->second(response.Result);
					    }
					    catch (const std::exception& ex)
					    {
						    std::cout << "Error calling application callback " << ex.what() << std::endl;
					    }
				    }
			    }
			    else if (response.Header.ServiceResult == OpcUa::StatusCode::BadSessionClosed)
			    {
				    if (Debug) 
				    {
					    std::cout << "BinaryClient | Session is closed";
				    }
			    }
			    else
			    {
				    // TODO
			    }
        });
      };
      
      Mutex.lock();
      Callbacks.insert(std::make_pair(request.Header.RequestHandle, std::make_pair(calculateTimeoutTime(request.Header.Timeout), responseCallback)));
      Mutex.unlock();

      Send(request);
      if (Debug) {std::cout << "binary_client| Publish  <--" << std::endl;}
    }

    virtual RepublishResponse Republish(const RepublishParameters& params)
    {
      if (Debug) {std::cout << "binary_client| Republish -->" << std::endl; }
      RepublishRequest request;
      CreateRequestHeader(request.Header);
      request.Parameters = params;

      RepublishResponse response = Send<RepublishResponse>(request);
      if (Debug) {std::cout << "binary_client| Republish  <--" << std::endl;}
      return response;
    }
    
    ////////////////////////////////////////////////////////////////
    /// View Services
    ////////////////////////////////////////////////////////////////
    virtual std::shared_ptr<ViewServices> Views() override
    {
      return shared_from_this();
    }

    virtual std::vector<BrowsePathResult> TranslateBrowsePathsToNodeIds(const TranslateBrowsePathsParameters& params) const
    {
      if (Debug)  { std::cout << "binary_client| TranslateBrowsePathsToNodeIds -->" << std::endl; }
      TranslateBrowsePathsToNodeIdsRequest request;
      CreateRequestHeader(request.Header);
      request.Parameters = params;
      const TranslateBrowsePathsToNodeIdsResponse response = Send<TranslateBrowsePathsToNodeIdsResponse>(request);
      if (Debug)  { std::cout << "binary_client| TranslateBrowsePathsToNodeIds <--" << std::endl; }
      return response.Result.Paths;
    }

    virtual std::vector<BrowseResult> Browse(const OpcUa::NodesQuery& query) const
    {
      if (Debug)  {
        std::cout << "binary_client| Browse -->" ;
        for ( BrowseDescription desc : query.NodesToBrowse )
        {
          std::cout << desc.NodeToBrowse << "  ";
        }
        std::cout << std::endl;
      }
      BrowseRequest request;
      CreateRequestHeader(request.Header);
      request.Query = query;
      const BrowseResponse response = Send<BrowseResponse>(request);
      for ( BrowseResult result : response.Results )
      {
        if (! result.ContinuationPoint.empty())
        {
          ContinuationPoints.push_back(result.ContinuationPoint);
        }
      }
      if (Debug)  { std::cout << "binary_client| Browse <--" << std::endl; }
      return  response.Results;
    }

    virtual std::vector<BrowseResult> BrowseNext() const
    {
      //FIXME: fix method interface so we do not need to decice arbitriraly if we need to send BrowseNext or not...
      if ( ContinuationPoints.empty() )
      {
        if (Debug)  { std::cout << "No Continuation point, no need to send browse next request" << std::endl; }
        return std::vector<BrowseResult>();
      }
      if (Debug)  { std::cout << "binary_client| BrowseNext -->" << std::endl; }
      BrowseNextRequest request;
      request.ReleaseContinuationPoints = ContinuationPoints.empty() ? true: false;
      request.ContinuationPoints = ContinuationPoints;
      const BrowseNextResponse response = Send<BrowseNextResponse>(request);
      if (Debug)  { std::cout << "binary_client| BrowseNext <--" << std::endl; }
      return response.Results;
    }

  virtual std::shared_ptr<OpcUa::AsyncUaClient> GetAsyncClient()
  {
    return shared_from_this();
  }

  private:
    //FIXME: this method should be removed, better add release option to BrowseNext
    void Release() const
    {
      ContinuationPoints.clear();
      BrowseNext();
    }

  public:

    ////////////////////////////////////////////////////////////////
    /// SecureChannel Services
    ////////////////////////////////////////////////////////////////
    virtual OpcUa::OpenSecureChannelResponse OpenSecureChannel(const OpenSecureChannelParameters& params)
    {
      if (Debug) {std::cout << "binary_client| OpenChannel -->" << std::endl;}

      OpenSecureChannelRequest request;
      request.Parameters = params;

      OpenSecureChannelResponse response = Send<OpenSecureChannelResponse>(request);

      ChannelSecurityToken = response.ChannelSecurityToken; //Save security token, we need it

      if (Debug) {std::cout << "binary_client| OpenChannel <--" << std::endl;}
      return response;
    }

    virtual void CloseSecureChannel(uint32_t channelId)
    {
      std::lock_guard<std::recursive_mutex> send_lock (ioSendMutex);
      try
      {
        if (Debug) {std::cout << "binary_client| CloseSecureChannel -->" << std::endl;}
        SecureHeader hdr(MT_SECURE_CLOSE, CHT_SINGLE, ChannelSecurityToken.SecureChannelId);

        const SymmetricAlgorithmHeader algorithmHeader = CreateAlgorithmHeader();
        hdr.AddSize(RawSize(algorithmHeader));

        const SequenceHeader sequence = CreateSequenceHeader();
        hdr.AddSize(RawSize(sequence));

        CloseSecureChannelRequest request;
        //request. ChannelId = channelId; FIXME: spec says it hsould be here, in practice it is not even sent?!?!
        hdr.AddSize(RawSize(request));

        Stream << hdr << algorithmHeader << sequence << request << flush;
        if (Debug) {std::cout << "binary_client| Secure channel closed." << std::endl;}
      }
      catch (const std::exception& exc)
      {
        std::cerr << "Closing secure channel failed with error: " << exc.what() << std::endl;
      }
      if (Debug) {std::cout << "binary_client| CloseSecureChannel <--" << std::endl;}
    }

private:
    template <typename Response, typename Request>
    Response Send(Request request) const
    {
      CreateRequestHeader(request.Header);

      std::shared_ptr<RequestCallback<Response>> requestCallback(new RequestCallback<Response>());
      ResponseCallback responseCallback = [requestCallback](std::vector<char> buffer, ResponseHeader h){
        requestCallback->OnData(std::move(buffer), std::move(h));
      };
      Mutex.lock();
      Callbacks.insert(std::make_pair(request.Header.RequestHandle, std::make_pair(calculateTimeoutTime(request.Header.Timeout), responseCallback)));
      Mutex.unlock();

      try
      {
        Send(request);
      }
      catch (std::exception ex)
      {
        Response result;
        result.Header.ServiceResult = OpcUa::StatusCode::BadCommunicationError;
        result.Header.RequestHandle = request.Header.RequestHandle;
        result.Header.Timestamp = request.Header.UtcTime;
        Mutex.lock();
        CallbackMap::iterator iter = Callbacks.find(request.Header.RequestHandle);
        if (iter != Callbacks.end())
        {
          Callbacks.erase(iter);
        }
        Mutex.unlock();
        return result;
      }
      return requestCallback->WaitForData(std::chrono::milliseconds(request.Header.Timeout));
    }

    template <typename Request>
    void Send(Request request) const
    {
      // TODO add support for breaking message into multiple chunks
      SecureHeader hdr(MT_SECURE_MESSAGE, CHT_SINGLE, ChannelSecurityToken.SecureChannelId);
      const SymmetricAlgorithmHeader algorithmHeader = CreateAlgorithmHeader();
      hdr.AddSize(RawSize(algorithmHeader));

      const SequenceHeader sequence = CreateSequenceHeader();
      hdr.AddSize(RawSize(sequence));
      hdr.AddSize(RawSize(request));

      {
        std::lock_guard<std::recursive_mutex> send_lock(ioSendMutex);
        Stream << hdr << algorithmHeader << sequence << request << flush;
      }
    }

    void InitializeRequestHeader(RequestHeader& requestHeader)
    {
      requestHeader.SessionAuthenticationToken = AuthenticationToken;
      requestHeader.RequestHandle = GetRequestHandle();
    };

    virtual std::shared_ptr<AsyncRequestContext<OpcUa::BrowseRequest, OpcUa::BrowseResponse>> beginSend(std::shared_ptr<OpcUa::BrowseRequest> request, std::function<bool(const std::shared_ptr<OpcUa::BrowseRequest>& request, std::shared_ptr<OpcUa::BrowseResponse> response)>callbackArg)
    {
      InitializeRequestHeader(request->Header);
      std::shared_ptr<AsyncRequestContext<BrowseRequest, BrowseResponse>> requestContext = std::make_shared<AsyncRequestContext<BrowseRequest, BrowseResponse>>(request, callbackArg);
      ResponseCallback responseCallback = [=](std::vector<char> buffer, ResponseHeader h){
        requestContext->OnDataReceived(std::move(buffer), std::move(h));
      };
      Mutex.lock();
      Callbacks.insert(std::make_pair(request->Header.RequestHandle, std::make_pair(calculateTimeoutTime(request->Header.Timeout), responseCallback)));
      Mutex.unlock();

      try
      {
        Send(*request);
      }
      catch (std::exception ex)
      {
        ResponseHeader responseHeader;
        responseHeader.ServiceResult = OpcUa::StatusCode::BadCommunicationError;
        responseHeader.RequestHandle = request->Header.RequestHandle;
        responseHeader.Timestamp = request->Header.UtcTime;
        requestContext->OnDataReceived(std::vector<char>(), std::move(responseHeader));
        Mutex.lock();
        CallbackMap::iterator iter = Callbacks.find(request->Header.RequestHandle);
        if (iter != Callbacks.end())
        {
          Callbacks.erase(iter);
        }
        Mutex.unlock();
      }
      return requestContext;
    }

    virtual std::shared_ptr<AsyncRequestContext<OpcUa::ReadRequest, OpcUa::ReadResponse>> beginSend(std::shared_ptr<OpcUa::ReadRequest> request, std::function<bool(const std::shared_ptr<OpcUa::ReadRequest>& request, std::shared_ptr<OpcUa::ReadResponse> response)>callbackArg)
    {
      InitializeRequestHeader(request->Header);

      std::shared_ptr<AsyncRequestContext<ReadRequest, ReadResponse>> requestContext = std::make_shared<AsyncRequestContext<ReadRequest, ReadResponse>>(request, callbackArg);
      ResponseCallback responseCallback = [requestContext](std::vector<char> buffer, ResponseHeader h){
        requestContext->OnDataReceived(std::move(buffer), std::move(h));
      };
      Mutex.lock();
      Callbacks.insert(std::make_pair(request->Header.RequestHandle, std::make_pair(calculateTimeoutTime(request->Header.Timeout), responseCallback)));
      Mutex.unlock();
      try
      {
        Send(*request);
      }
      catch (std::exception ex)
      {
        ResponseHeader responseHeader;
        responseHeader.ServiceResult = OpcUa::StatusCode::BadCommunicationError;
        responseHeader.RequestHandle = request->Header.RequestHandle;
        responseHeader.Timestamp = request->Header.UtcTime;
        requestContext->OnDataReceived(std::vector<char>(), std::move(responseHeader));
        Mutex.lock();
        CallbackMap::iterator iter = Callbacks.find(request->Header.RequestHandle);
        if (iter != Callbacks.end())
        {
          Callbacks.erase(iter);
        }
        Mutex.unlock();
      }      return requestContext;
    }

    virtual std::shared_ptr<AsyncRequestContext<OpcUa::CreateSubscriptionRequest, OpcUa::CreateSubscriptionResponse>> beginSend(std::shared_ptr<OpcUa::CreateSubscriptionRequest> request, std::function<bool(const std::shared_ptr<OpcUa::CreateSubscriptionRequest>& request, std::shared_ptr<OpcUa::CreateSubscriptionResponse> response)>callbackArg)
    {
      InitializeRequestHeader(request->Header);

      std::shared_ptr<AsyncRequestContext<CreateSubscriptionRequest, CreateSubscriptionResponse>> requestContext = std::make_shared<AsyncRequestContext<CreateSubscriptionRequest, CreateSubscriptionResponse>>(request, callbackArg);
      ResponseCallback responseCallback = [requestContext](std::vector<char> buffer, ResponseHeader h){
        requestContext->OnDataReceived(std::move(buffer), std::move(h));
      };
      Mutex.lock();
      Callbacks.insert(std::make_pair(request->Header.RequestHandle, std::make_pair(calculateTimeoutTime(request->Header.Timeout), responseCallback)));
      Mutex.unlock();
      try
      {
        Send(*request);
      }
      catch (std::exception ex)
      {
        ResponseHeader responseHeader;
        responseHeader.ServiceResult = OpcUa::StatusCode::BadCommunicationError;
        responseHeader.RequestHandle = request->Header.RequestHandle;
        responseHeader.Timestamp = request->Header.UtcTime;
        requestContext->OnDataReceived(std::vector<char>(), std::move(responseHeader));
        Mutex.lock();
        CallbackMap::iterator iter = Callbacks.find(request->Header.RequestHandle);
        if (iter != Callbacks.end())
        {
          Callbacks.erase(iter);
        }
        Mutex.unlock();
      }
      return requestContext;
    }

    virtual std::shared_ptr<AsyncRequestContext<OpcUa::CreateMonitoredItemsRequest, OpcUa::CreateMonitoredItemsResponse>> beginSend(std::shared_ptr<OpcUa::CreateMonitoredItemsRequest> request, std::function<bool(const std::shared_ptr<OpcUa::CreateMonitoredItemsRequest>& request, std::shared_ptr<OpcUa::CreateMonitoredItemsResponse> response)>callbackArg)
    {
      InitializeRequestHeader(request->Header);

      std::shared_ptr<AsyncRequestContext<CreateMonitoredItemsRequest, CreateMonitoredItemsResponse>> requestContext = std::make_shared<AsyncRequestContext<CreateMonitoredItemsRequest, CreateMonitoredItemsResponse>>(request, callbackArg);
      ResponseCallback responseCallback = [requestContext](std::vector<char> buffer, ResponseHeader h){
        requestContext->OnDataReceived(std::move(buffer), std::move(h));
      };
      Mutex.lock();
      Callbacks.insert(std::make_pair(request->Header.RequestHandle, std::make_pair(calculateTimeoutTime(request->Header.Timeout), responseCallback)));
      Mutex.unlock();
      try
      {
        Send(*request);
      }
      catch (std::exception ex)
      {
        ResponseHeader responseHeader;
        responseHeader.ServiceResult = OpcUa::StatusCode::BadCommunicationError;
        responseHeader.RequestHandle = request->Header.RequestHandle;
        responseHeader.Timestamp = request->Header.UtcTime;
        requestContext->OnDataReceived(std::vector<char>(), std::move(responseHeader));
        Mutex.lock();
        CallbackMap::iterator iter = Callbacks.find(request->Header.RequestHandle);
        if (iter != Callbacks.end())
        {
          Callbacks.erase(iter);
        }
        Mutex.unlock();
      }
      return requestContext;
    }

    virtual std::shared_ptr<AsyncRequestContext<OpcUa::PublishRequest, OpcUa::PublishResponse>> beginSend(
      std::shared_ptr<OpcUa::PublishRequest> request, std::function<bool(const std::shared_ptr<OpcUa::PublishRequest>& request, std::shared_ptr<OpcUa::PublishResponse> response)>callbackArg)
    {
      InitializeRequestHeader(request->Header);

      std::shared_ptr<AsyncRequestContext<PublishRequest, PublishResponse>> requestContext = std::make_shared<AsyncRequestContext<PublishRequest, PublishResponse>>(request, callbackArg);
      ResponseCallback responseCallback = [requestContext](std::vector<char> buffer, ResponseHeader h){
        requestContext->OnDataReceived(std::move(buffer), std::move(h));
      };
      Mutex.lock();
      Callbacks.insert(std::make_pair(request->Header.RequestHandle, std::make_pair(calculateTimeoutTime(request->Header.Timeout), responseCallback)));
      Mutex.unlock();
      try
      {
        Send(*request);
      }
      catch (std::exception ex)
      {
        ResponseHeader responseHeader;
        responseHeader.ServiceResult = OpcUa::StatusCode::BadCommunicationError;
        responseHeader.RequestHandle = request->Header.RequestHandle;
        responseHeader.Timestamp = request->Header.UtcTime;
        requestContext->OnDataReceived(std::vector<char>(), std::move(responseHeader));
        Mutex.lock();
        CallbackMap::iterator iter = Callbacks.find(request->Header.RequestHandle);
        if (iter != Callbacks.end())
        {
          Callbacks.erase(iter);
        }
        Mutex.unlock();
      }
      return requestContext;
    }

    bool Receive() const
    {
      bool result = false;
      ResponseHeader header;
      std::vector<char> buffer;
      NodeId id;
      std::lock_guard<std::recursive_mutex> receiveLock(ioReceiveMutex);
      try
      {
        // Message structure is:
        // 1. MessageHeader;
        // 2. SecurityHeader
        // 3. SequenceHeader
        // 4. Body
        // 5. Padding
        // 6. Signature

        Binary::SecureHeader messageHeader;
        // 1. Read MessageHeader
        Stream >> messageHeader;

        // Now read the rest of the message into the buffer, so next message can be read event this one cannot be read:
        const std::size_t messageHeaderSize = RawSize(messageHeader);

        // 2. Read SecurityHeader:
        size_t algo_size;
        if (messageHeader.Type == MessageType::MT_SECURE_OPEN)
        {
          AsymmetricAlgorithmHeader responseAlgo;
          Stream >> responseAlgo;
          algo_size = RawSize(responseAlgo);
        }
        else if (messageHeader.Type == MessageType::MT_ERROR)
        {
          // If message type is error, then message structure is different:
          //StatusCode error;
          //std::string msg;
          //Stream >> error;
          //Stream >> msg;
          //std::stringstream stream;
          throw std::runtime_error("Received TCP message of type ERR from server");
        }
        else //(responseHeader.Type == MessageType::MT_SECURE_MESSAGE )
        {
          // 2. Another type of SecurityHeader:
          Binary::SymmetricAlgorithmHeader responseAlgo;
          Stream >> responseAlgo;
          algo_size = RawSize(responseAlgo);
        }

        // 3. Read SequenceHeader:
        Binary::SequenceHeader responseSequence;
        Stream >> responseSequence; // TODO Check for request Number

        const std::size_t messageBodySize = messageHeader.Size - messageHeaderSize;
        size_t sequenceHeaderSize = RawSize(responseSequence);
        std::size_t responseDataSize = messageHeader.Size - messageHeaderSize - algo_size - sequenceHeaderSize;
        buffer.resize(responseDataSize);
        BufferInputChannel bufferInput(buffer);
        Binary::RawBuffer raw(&buffer[0], responseDataSize);
        Stream >> raw;
        IStreamBinary in(bufferInput);

        // 4. Read Response Body partially:
        in >> id;       // 4.1 Message Id
        in >> header;   // 4.2 Response header
        if (header.RequestHandle != responseSequence.RequestId)
        {
          if (Debug)std::cout << "binary_client| Receive - request handle in SequenceHEader " << responseSequence.RequestId <<
            "does not match with the one in response header: " << header.RequestHandle << std::endl;
        }
        if (Debug)std::cout << "binary_client| Got response id: " << id << " and handle " << header.RequestHandle << std::endl;

        if (( ((uint32_t) header.ServiceResult) & 0xC0000000) != 0)
        {
          std::cout << "binary_client| Received a response from server with error status: " << OpcUa::ToString(header.ServiceResult) << std::endl;
        }

        if (id == SERVICE_FAULT)
        {
          std::cerr << std::endl;
          std::cerr << "Receive ServiceFault from Server with StatusCode " << OpcUa::ToString(header.ServiceResult) << std::endl;
          std::cerr << std::endl;
        }
        result = true;
      }
      catch (std::exception ex)
      {
        OpcUa::StatusCode serviceResultForCallbacks = StatusCode::BadCommunicationError;
        switch (ConnectionState)
        {
        case ClientConnectionState::Connecting:
        case ClientConnectionState::Reconnecting:
          ConnectionState = ClientConnectionState::CouldNotConnect;
          //statusChangeCallback(ConnectionState, StatusCode::BadCommunicationError, ex.what());
          break;
        case ClientConnectionState::Disconnecting:
          ConnectionState = ClientConnectionState::Disconnected;
          serviceResultForCallbacks = StatusCode::BadDisconnect;
          //statusChangeCallback(ConnectionState, StatusCode::BadCommunicationError, ex.what());
          break;
        case ClientConnectionState::Connected:
        case ClientConnectionState::Reconnected:
          {
            std::string errorMessage = ex.what();
            if (errorMessage == "Connection was closed by host.")
            {
              ConnectionState = ClientConnectionState::ConnectionClosedByServer;
              serviceResultForCallbacks = StatusCode::BadConnectionClosed;
            }
            else
            {
              ConnectionState = ClientConnectionState::CommunicationError;
            }
            statusChangeCallback(ConnectionState, StatusCode::BadCommunicationError, errorMessage);
          }
          break;
        }

        // Call callbacks for all pending requests:
        {
          Mutex.lock();
          std::vector<char> emptyBuffer;
          for (CallbackMap::const_iterator callbackIt = Callbacks.begin(); callbackIt != Callbacks.end();)
          {
            header.RequestHandle = callbackIt->first;
            header.ServiceResult = serviceResultForCallbacks;
            try
            {
              std::cout << "Removing request with id = " << header.RequestHandle << " from callbacks queue due to the Receive thread stopping" << std::endl;
              callbackIt->second.second(emptyBuffer, header);
            }
            catch (std::exception& ex)
            {
              int i = 0;
              i++;
            }
            catch (...)
            {
              int i = 0;
              i++;
            }
            callbackIt = Callbacks.erase(callbackIt);
          }
          Mutex.unlock();
        }
        //throw ex;
      }

      if (result)
      {
        Mutex.lock();
        CallbackMap::const_iterator callbackIt = Callbacks.find(header.RequestHandle);
        if (callbackIt == Callbacks.end())
        {
          std::cout << "binary_client| No callback found for message with id: " << id << " and handle " << header.RequestHandle << std::endl;
        }
        else
        {
          if (buffer.size() == 0)
          {
            header.ServiceResult = OpcUa::StatusCode::BadCommunicationError;
          }
          // TODO - Probably it is better to call function outside of the lock
          try
          {
            callbackIt->second.second(std::move(buffer), std::move(header));
          }
          catch (std::exception ex)
          {
            std::cout << "binary-client| Exception caught in Receive callback: " << ex.what() << "\n";
          }
          catch (uint32_t ex)
          {
            std::cout << "binary-client| Integer type exception caught in Receive callback: " << ex << "\n";
          }
          catch (...)
          {
            std::cout << "binary-client| unknown type exception caught in Receive callback" << "\n";
          }
          Callbacks.erase(callbackIt);
        }
        Mutex.unlock();
      }

      return result;
    }

    Binary::Acknowledge HelloServer(const SecureConnectionParams& params)
    {
      if (Debug) {std::cout << "binary_client| HelloServer -->" << std::endl;}

      Acknowledge ack;

      std::lock_guard<std::recursive_mutex> send_lock(ioSendMutex);
      try
      {
        Binary::Hello hello;
        hello.ProtocolVersion = 0;
        hello.ReceiveBufferSize = 0xFFFFFF; // Min. value is 8192
        hello.SendBufferSize =    0xFFFFFF; // Min. value is 8192
        hello.MaxMessageSize = 0; // 0 means no limit
        hello.MaxChunkCount = 1; // Currently there is no support for chunks, so set it to 1
        hello.EndpointUrl = params.EndpointUrl;

        Binary::Header hdr(Binary::MT_HELLO, Binary::CHT_SINGLE);
        hdr.AddSize(RawSize(hello));

        Stream << hdr << hello << flush;

        Header respHeader;
        Stream >> respHeader; // TODO add check for acknowledge header
        switch (respHeader.Type)
        {
        case Binary::MT_ACKNOWLEDGE:
          Stream >> ack; // TODO check for connection parameters
          break;
        case Binary::MT_ERROR:
          {
            uint32_t errorCode;
            std::string errorMessage;
            Stream >> errorCode;
            Stream >> errorMessage;
            if (Debug) { std::cout << "binary_client| HelloServer <-- Server returned error: code = " << errorCode << ", message = " << errorMessage << std::endl; }
            throw "binary_client|HelloServer: Server returned error message instead of ACK";
          }
          break;
        default:
          if (Debug) { std::cout << "binary_client| HelloServer <-- Server returned unexpected message type " << respHeader.Type << std::endl; }
          throw "binary_client|HelloServer: Server returned unexpected message type";
          break;
        }
      }
      catch (std::exception ex)
      {
        CloseConnection();
        throw ex;
      }
      if (Debug) {std::cout << "binary_client| HelloServer <--" << std::endl;}

      return ack;
    }


    SymmetricAlgorithmHeader CreateAlgorithmHeader() const
    {
      SymmetricAlgorithmHeader algorithmHeader;
      algorithmHeader.TokenId = ChannelSecurityToken.TokenId;
      return algorithmHeader;
    }

    SequenceHeader CreateSequenceHeader() const
    {
      SequenceHeader sequence;
      sequence.SequenceNumber = ++SequenceNumber;
      sequence.RequestId = ++RequestNumber;
      return sequence;
    }

    void CreateRequestHeader(RequestHeader& header) const
    {
      header.SessionAuthenticationToken = AuthenticationToken;
      header.RequestHandle = GetRequestHandle();
      header.Timeout = 10000;
      return;
    }

    unsigned GetRequestHandle() const
    {
      return ++RequestHandle;
    }

    std::chrono::time_point<std::chrono::system_clock> calculateTimeoutTime(uint32_t timeout) const 
    {
      if (timeout > 0)
      {
        return std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout);
      }
      else
      {
        return std::chrono::steady_clock::now() + std::chrono::hours(1);
      }
    }

  private:
    std::shared_ptr<IOChannel> Channel;
    mutable IOStreamBinary Stream;
    SecureConnectionParams Params;
    std::thread ReceiveThread;

    SubscriptionCallbackMap PublishCallbacks;
    SecurityToken ChannelSecurityToken;
    mutable std::atomic<uint32_t> SequenceNumber;
    mutable std::atomic<uint32_t> RequestNumber;
    ExpandedNodeId AuthenticationToken;
    mutable std::atomic<uint32_t> RequestHandle;
    mutable std::vector<std::vector<uint8_t>> ContinuationPoints;
    mutable CallbackMap Callbacks;

    std::chrono::system_clock::time_point LastMessageReceivedTime;
    std::chrono::system_clock::time_point LastCallbacksCleanTime;

    const bool Debug = true;
    std::atomic<bool> Finished = false;

    std::thread callback_thread;
    CallbackThread CallbackService;
    
    mutable std::recursive_mutex Mutex;
    mutable std::recursive_mutex ioSendMutex;
    mutable std::recursive_mutex ioReceiveMutex;

    ConnectionStatusChangeCallback statusChangeCallback;
    mutable std::atomic<ClientConnectionState> ConnectionState;

    std::thread requestQueueCleanerThread;
  };

  template <>
  void BinaryClient::Send<OpenSecureChannelRequest>(OpenSecureChannelRequest request) const
  {
    SecureHeader hdr(MT_SECURE_OPEN, CHT_SINGLE, ChannelSecurityToken.SecureChannelId);
    AsymmetricAlgorithmHeader algorithmHeader;
    algorithmHeader.SecurityPolicyURI = Params.SecurePolicy;
    algorithmHeader.SenderCertificate = Params.SenderCertificate;
    algorithmHeader.ReceiverCertificateThumbPrint = Params.ReceiverCertificateThumbPrint;
    hdr.AddSize(RawSize(algorithmHeader));
    hdr.AddSize(RawSize(request));

    const SequenceHeader sequence = CreateSequenceHeader();
    hdr.AddSize(RawSize(sequence));
    {
      std::lock_guard<std::recursive_mutex> send_lock(ioSendMutex);
      Stream << hdr << algorithmHeader << sequence << request << flush;
    }
  }
} // namespace

OpcUa::Services::SharedPtr OpcUa::CreateBinaryClient(OpcUa::IOChannel::SharedPtr channel, const OpcUa::SecureConnectionParams& params, bool debug, ConnectionStatusChangeCallback callback)
{
  return std::make_shared<BinaryClient>(channel, params, debug, callback);
}

OpcUa::Services::SharedPtr OpcUa::CreateBinaryClient(const std::string& endpointUrl, bool debug, ConnectionStatusChangeCallback callback)
{
  const Common::Uri serverUri(endpointUrl);
  OpcUa::IOChannel::SharedPtr channel = OpcUa::Connect(serverUri.Host(), serverUri.Port());
  OpcUa::SecureConnectionParams params;
  params.EndpointUrl = endpointUrl;
  params.SecurePolicy = "http://opcfoundation.org/UA/SecurityPolicy#None";
  return CreateBinaryClient(channel, params, debug, callback);
}

namespace OpcUa
{
  ConnectionStatusChangeCallback OpcUa::defaultCallback = [](ClientConnectionState state, OpcUa::StatusCode statusCode, const std::string& errorMessage)
  {
    return 1000; 
  };
}