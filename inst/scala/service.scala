package sparklyr

import javax.servlet.ServletException
import javax.servlet.http.HttpServletRequest
import javax.servlet.http.HttpServletResponse

import javax.ws.rs.GET
import javax.ws.rs.Path
import javax.ws.rs.Produces
import javax.ws.rs.core.MediaType

import org.eclipse.jetty.server.Server
import org.eclipse.jetty.servlet.ServletContextHandler
import org.eclipse.jetty.servlet.ServletHolder

import com.sun.jersey.spi.container.servlet.ServletContainer

import org.eclipse.jetty.servlet

@Path("/")
class EntryPoint {
  @GET
  @Path("hello")
  @Produces(Array(MediaType.TEXT_PLAIN))
  def test(): String = {
    return "Hello Wold!";
  }
}

class Service {
  var server_ = new Server(8097)

  def run(): Unit = {
    var handler: ServletContextHandler = new ServletContextHandler(
      server_,
      "/",
      ServletContextHandler.SESSIONS)
    
    val holder:ServletHolder = new ServletHolder(classOf[ServletContainer])
    
    holder.setInitParameter("com.sun.jersey.config.property.packages",
                            "sparklyr")
                            
    handler.addServlet(holder, "/*");
    
    server_.start()
  }

  def stop(): Unit = {
    server_.stop()
  }
}
