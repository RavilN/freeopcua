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
  public:
    AsyncRequestContext(std::shared_ptr<Request> requestToExecute, std::function<bool(const std::shared_ptr<Request> request, std::shared_ptr<Response> response)> callbackFunctionArg)
      : lock(m)
    {
      this->request = requestToExecute;
      this->response = std::make_shared<Response>();
      callbackFunction = callbackFunctionArg;
    }

    void OnDataReceived(std::vector<char> data, OpcUa::ResponseHeader h)
    {
      response->Header = std::move(h);
      if (data.empty())
      {
        std::cout << "Error: Received packet with empty body from server" << std::endl;
      }
      else
      {
        try
        {
          BufferInputChannel bufferInput(data);
          IStreamBinary in(bufferInput);
          in >> *response;
        }
        catch (std::exception ex)
        {
          response->Header.ServiceResult = OpcUa::StatusCode::BadCommunicationError;
          //TODO: log error, add diagnostic info to the response
        }
      }
      callbackFunction(request, response);
      doneEvent.notify_all();
    }

    std::shared_ptr<Response> WaitForCompletion(std::chrono::milliseconds msec)
    {
      if (msec == 0)
      {
        msec = 0x7FFFFFFF; //TODO - put proper max value
      }
      doneEvent.wait_for(lock, msec);

      return response;
    }

  protected:
    std::shared_ptr<Request> request;
    std::shared_ptr<Response> response;
    std::function<bool(const std::shared_ptr<Request>& request, std::shared_ptr<Response> response)> callbackFunction;
  private:
    std::mutex m;
    std::unique_lock<std::mutex> lock;
    std::condition_variable doneEvent;
  };

  class AsyncUaClient
  {
  public:
    virtual ~AsyncUaClient(){}
    virtual std::shared_ptr<AsyncRequestContext<OpcUa::BrowseRequest, OpcUa::BrowseResponse>> beginSend(std::shared_ptr<OpcUa::BrowseRequest> request, std::function<bool(const std::shared_ptr<OpcUa::BrowseRequest>& request, std::shared_ptr<OpcUa::BrowseResponse> response)>callbackArg) { return 0; }
    virtual std::shared_ptr<AsyncRequestContext<OpcUa::ReadRequest, OpcUa::ReadResponse>> beginSend(std::shared_ptr<OpcUa::ReadRequest> request, std::function<bool(const std::shared_ptr<OpcUa::ReadRequest>& request, std::shared_ptr<OpcUa::ReadResponse> response)>callbackArg) { return 0; }
  };
}