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

import org.glassfish.jersey.servlet.ServletContainer

@Path("/entry-point")
class EntryPoint {
    @GET
    @Path("test")
    @Produces(Array(MediaType.TEXT_PLAIN))
    def test(): String = {
        return "Test";
    }
}

class Service {
  var server_ = new Server(8097)

  def run(): Unit = {
    var context: ServletContextHandler = new ServletContextHandler(
      ServletContextHandler.SESSIONS)
    
    context.setContextPath("/")
    server_.setHandler(context)
    
    var serverlet = context.addServlet(classOf[ServletContainer], "/*");
    serverlet.setInitOrder(0)
    
    serverlet.setInitParameter(
      "jersey.config.server.provider.classnames",
      classOf[EntryPoint].getCanonicalName())
        
    server_.start()
  }

  def stop(): Unit = {
    server_.stop()
  }
}
