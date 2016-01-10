#pragma once

#include <memory>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <queue>
#include <thread>
#include <iostream>
#include <vector>
#include "opc/ua/protocol/types.h"

namespace OpcUa
{
  template <typename Request, typename Response>
  class AsyncRequestContext
  {
  private:
    AsyncRequestContext(){}
    AsyncRequestContext(AsyncRequestContext&){}

  public:
    AsyncRequestContext(std::shared_ptr<Request> requestToExecute, std::function<bool(const std::shared_ptr<Request> request, std::shared_ptr<Response> response)> callbackFunctionArg)
    {
      this->request = requestToExecute;
      this->response = std::make_shared<Response>();
      callbackFunction = callbackFunctionArg;
      isPending = true;
    }
    ~AsyncRequestContext()
    {
      {
        std::lock_guard<std::mutex> lock(m);
        if (isPending)
        {
          response->Header.ServiceResult = OpcUa::StatusCode::BadRequestCancelledByClient;
          callbackFunction(request, response);
          isPending = false;
        }
      }
      doneEvent.notify_all();
    }

    void OnDataReceived(std::vector<char> data, OpcUa::ResponseHeader h)
    {
      {
        std::lock_guard<std::mutex> lock(m);
        if (isPending)
        {
          response->Header = std::move(h);
          if ((((uint32_t)response->Header.ServiceResult) & 0xC0000000) == 0)
          {
            try
            {
              BufferInputChannel bufferInput(data);
              IStreamBinary in(bufferInput);
              in >> *response;
            }
            catch (std::exception ex)
            {
              response->Header.ServiceResult = OpcUa::StatusCode::BadDecodingError;
              std::cout << "Error: response could not be decoded: exception = " << ex.what() << std::endl;
              //TODO: add diagnostic info to the response
            }
          }
          callbackFunction(request, response);
          isPending = false;
        }
      }
      doneEvent.notify_all();
    }
    std::shared_ptr<Response> WaitForCompletion(std::chrono::milliseconds msec)
    {
      if (msec.count() == 0)
      {
        msec = std::chrono::milliseconds(0x7FFFFFFF); //TODO - put proper max value
      }
      std::unique_lock<std::mutex> lock(m);
      {
        doneEvent.wait_for(lock, msec, []{return !isPending; });
      }
      return response;
    }


  protected:
    std::shared_ptr<Request> request;
    std::shared_ptr<Response> response;
    std::function<bool(const std::shared_ptr<Request>& request, std::shared_ptr<Response> response)> callbackFunction;
  private:
    std::mutex m;
    std::condition_variable doneEvent;
    bool isPending;
  };

  
  class AsyncUaClient
  {
  public:
    virtual ~AsyncUaClient() { }
    virtual std::shared_ptr<AsyncRequestContext<OpcUa::BrowseRequest, OpcUa::BrowseResponse>> beginSend(std::shared_ptr<OpcUa::BrowseRequest> request, std::function<bool(const std::shared_ptr<OpcUa::BrowseRequest>& request, std::shared_ptr<OpcUa::BrowseResponse> response)>callbackArg) { return 0; }
    virtual std::shared_ptr<AsyncRequestContext<OpcUa::ReadRequest, OpcUa::ReadResponse>> beginSend(std::shared_ptr<OpcUa::ReadRequest> request, std::function<bool(const std::shared_ptr<OpcUa::ReadRequest>& request, std::shared_ptr<OpcUa::ReadResponse> response)>callbackArg) { return 0; }
    virtual std::shared_ptr<AsyncRequestContext<OpcUa::CreateSubscriptionRequest, OpcUa::CreateSubscriptionResponse>> beginSend(std::shared_ptr<OpcUa::CreateSubscriptionRequest> request, std::function<bool(const std::shared_ptr<OpcUa::CreateSubscriptionRequest>& request, std::shared_ptr<OpcUa::CreateSubscriptionResponse> response)>callbackArg) { return 0; }
    virtual std::shared_ptr<AsyncRequestContext<OpcUa::CreateMonitoredItemsRequest, OpcUa::CreateMonitoredItemsResponse>> beginSend(std::shared_ptr<OpcUa::CreateMonitoredItemsRequest> request, std::function<bool(const std::shared_ptr<OpcUa::CreateMonitoredItemsRequest>& request, std::shared_ptr<OpcUa::CreateMonitoredItemsResponse> response)>callbackArg) { return 0; }
    virtual std::shared_ptr<AsyncRequestContext<OpcUa::PublishRequest, OpcUa::PublishResponse>> beginSend(std::shared_ptr<OpcUa::PublishRequest> request, std::function<bool(const std::shared_ptr<OpcUa::PublishRequest>& request, std::shared_ptr<OpcUa::PublishResponse> response)>callbackArg) { return 0; }
    virtual void InitializeRequestHeader(RequestHeader& requestHeader) {};
  };
}