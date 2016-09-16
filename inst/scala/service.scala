package sparklyr

import org.mortbay.jetty.Server

class Service {
  def run(): Unit = {
    var server:Server = new Server(8097)
    server.start()
    server.join()
  }
}
