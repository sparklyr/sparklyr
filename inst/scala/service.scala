package sparklyr

import javax.servlet.ServletException
import javax.servlet.http.HttpServletRequest
import javax.servlet.http.HttpServletResponse

import org.mortbay.jetty.Request
import org.mortbay.jetty.Server
import org.mortbay.jetty.handler.AbstractHandler

class ServiceHandler extends AbstractHandler {
  var html = <h1>Hello World</h1>

  def handle(target: String, request: HttpServletRequest, response: HttpServletResponse, i: Int) = {
    response.setContentType("text/html")
    response.setStatus(HttpServletResponse.SC_OK)
    response.getWriter().println(html.toString())
    (request.asInstanceOf[Request]).setHandled(true)
  }
}

class Service {
  var server_ = new Server(8097)

  def run(): Unit = {
    server_.setHandler(new ServiceHandler())
    server_.start()
    server_.join()
  }

  def stop(): Unit = {
    server_.stop()
  }
}
