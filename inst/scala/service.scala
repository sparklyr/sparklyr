package sparklyr

import javax.servlet.ServletException
import javax.servlet.http.HttpServletRequest
import javax.servlet.http.HttpServletResponse

import org.mortbay.jetty.Request
import org.mortbay.jetty.Server
import org.mortbay.jetty.handler.AbstractHandler

class ServiceHandler extends AbstractHandler {
  def handle(target: String, request: HttpServletRequest, response: HttpServletResponse, i: Int) = {
    var html = "sparklyr: " + java.util.Calendar.getInstance.getTime.toString

    response.setContentType("text/html")
    response.setStatus(HttpServletResponse.SC_OK)
    response.getWriter().println(html)
    (request.asInstanceOf[Request]).setHandled(true)
  }
}

class Service {
  var server_ = new Server(8097)

  def run(): Unit = {
    server_.setHandler(new ServiceHandler())
    server_.start()
  }

  def stop(): Unit = {
    server_.stop()
  }
}
