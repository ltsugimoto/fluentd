#
# Fluentd
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
#

require 'fluent/plugin_helper/event_loop'
require 'fluent/plugin_helper/timer'

require 'serverengine/socket_manager'
require 'cool.io'
require 'socket'
require 'fcntl'

module Fluent
  module PluginHelper
    module Server
      include Fluent::PluginHelper::EventLoop
      include Fluent::PluginHelper::Timer

      # stop     : [-]
      # shutdown : [-]
      # close    : [-]
      # terminate: [-]

      attr_reader :_servers # for tests

      def server_wait_until_start
      end

      def server_wait_until_stop
      end

      PROTOCOLS = [:tcp, :udp, :tls, :unix]
      CONNECTION_PROTOCOLS = [:tcp, :tls, :unix]

      # server_create_connection(:title, @port) do |conn|
      #   # on connection
      #   source_addr = conn.addr
      #   source_port = conn.port
      #   conn.data do |data|
      #     # on data
      #     conn.write resp # ...
      #     conn.disconnect
      #   end
      # end
      def server_create_connection(title, port, proto: :tcp, bind: '0.0.0.0', shared: true, certopts: nil, linger_timeout: 0, backlog: nil, &block)
        raise ArgumentError, "BUG: title must be a symbol" unless title.is_a? Symbol
        raise ArgumentError, "BUG: cannot create connection for UDP" unless CONNECTION_PROTOCOLS.include?(proto)
        raise ArgumentError, "BUG: block not specified which handles connection" unless block_given?
        raise ArgumentError, "BUG: block must have just one argument" unless block.arity == 1

        case proto
        when :tcp
          server = server_create_for_tcp_connection(shared, bind, port, resolve_name, linger_timeout, backlog, &block)
        when :tls
          raise ArgumentError, "BUG: certopts (certificate options) not specified for TLS" unless certopts
          server_certopts_validate!(certopts)
          sock = server_create_tls_socket(shared, bind, port)
          server = nil # ...
          raise "not implemented yet"
        when :unix
          raise "not implemented yet"
        else
          raise "unknown protocol #{proto}"
        end

        event_loop_attach(server)
      end

      # server_create(:title, @port) do |data|
      #   # ...
      # end
      # server_create(:title, @port) do |data, conn|
      #   # ...
      # end
      # server_create(:title, @port, proto: :udp, max_bytes: 2048) do |data, addr, sock|
      #   # ...
      # end
      def server_create(title, port, proto: :tcp, bind: '0.0.0.0', shared: true, certopts: nil, max_bytes: nil, flags: 0, &callback)
        raise ArgumentError, "BUG: title must be a symbol" unless title.is_a? Symbol
        raise ArgumentError, "BUG: invalid protocol name" unless PROTOCOLS.include?(proto)
        raise ArgumentError, "BUG: block not specified which handles received data" unless block_given?

        case proto
        when :tcp
          raise ArgumentError, "BUG: block must have 1 or 2 arguments" unless block.arity == 1 || block.arity == 2
          server = server_create_for_tcp_connection(shared, bind, port, resolve_name, linger_timeout, backlog) do |conn|
            conn.data(&callback)
          end
        when :tls
          raise ArgumentError, "BUG: block must have 1 or 2 arguments" unless block.arity == 1 || block.arity == 2
          raise ArgumentError, "BUG: certopts (certificate options) not specified for TLS" unless certopts
          server_certopts_validate!(certopts)
          raise "not implemented yet"
        when :udp
          raise ArgumentError, "BUG: block must have arguments upto 3" unless block.arity >= 1 && block.arity <= 3
          raise ArgumentError, "BUG: max_bytes must be specified for UDP" unless max_bytes
          sock = server_create_udp_socket(shared, bind, port)
          server = UDPServerHandler.new(sock, max_bytes, @log, &callback)
        when :unix
          raise "not implemented yet"
        else
          raise "BUG: unknown protocol #{proto}"
        end

        event_loop_attach(server)
      end

      def server_create_for_tcp_connection(shared, bind, port, resolve_name, linger_timeout, backlog, &block)
        sock = server_create_tcp_socket(shared, bind, port)
        server = Coolio::TCPServer.new(sock, nil, TCPServerHandler, resolve_name, linger_timeout, @log, &block)
        server.listen(backlog) if backlog
        server
      end

      def initialize
        super
        @_servers = []
      end

      def stop
        super
      end

      def shutdown
        super
      end

      def close
        super
      end

      def terminate
        super
      end

      def server_certopts_validate!(certopts)
        raise "not implemented yet"
      end

      def server_socket_manager_client
        socket_manager_path = ENV['SERVERENGINE_SOCKETMANAGER_PATH']
        if Fluent.windows?
          socket_manager_path = socket_manager_path.to_i
        end
        ServerEngine::SocketManager::Client.new(socket_manager_path)
      end

      def server_create_tcp_socket(shared, bind, port)
        sock = if shared
                 server_socket_manager_client.listen_tcp(bind, port)
               else
                 TCPServer.new(bind, port)
               end
        sock.fcntl(Fcntl::F_SETFD, Fcntl::FD_CLOEXEC) # close-on-exec
        sock
      end

      def server_create_udp_socket(shared, bind, port)
        sock = if shared
                 server_socket_manager_client.listen_udp(bind, port)
               else
                 usock = UDPSocket.new
                 usock.bind(bind, port)
               end
        sock.fcntl(Fcntl::F_SETFD, Fcntl::FD_CLOEXEC) # close-on-exec
        sock.fcntl(Fcntl::F_SETFL, Fcntl::O_NONBLOCK) # nonblock
        sock
      end

      def server_create_tls_socket(shared, bind, port)
        raise "not implemented yet"
      end

      # TODO: add title

      class UDPServerHandler < Coolio::IO
        def initialize(io, max_bytes, log, callback)
          super(sock)
          @sock = sock
          @max_bytes = max_bytes
          @log = log
          @callback = callback
        end

        def on_readable
          begin
            data, addr = @sock.recvfrom(@max_bytes)
          rescue Errno::EAGAIN, Errno::EWOULDBLOCK, Errno::EINTR
            return
          end
          # TODO: arity check
          @callback.call(data, addr, @sock)
        rescue => e
          @log.error "unexpected error in processing UDP data", error: e
          @log.error_backtrace
        end
      end

      class TCPServerHandler < Coolio::Socket
        attr_reader :protocol, :remote_port, :remote_addr, :remote_host

        PEERADDR_FAILED = ["?", "?", "name resolusion failed", "?"]

        def initialize(sock, resolve_name, linger_timeout, log, connect_callback)
          raise ArgumentError, "socket is a TCPSocket" unless sock.is_a?(TCPSocket)

          super(io)

          @log = log
          @connect_callback = connect_callback
          @data_callback = nil

          opt = [1, linger_timeout].pack('I!I!')  # { int l_onoff; int l_linger; }
          io.setsockopt(Socket::SOL_SOCKET, Socket::SO_LINGER, opt)

          # TODO: resolve_name
          if @resolve_name
            # ....
          end
          @peeraddr = (io.peeraddr rescue PEERADDR_FAILED)
          if addr == '?'
            port, addr = *Socket.unpack_sockaddr_in(io.getpeername) rescue nil
            @peeraddr = [nil, port, nil, addr] # proto, port, host, addr
          end
          # TODO: remove_addr & remote_port ? or peeraddr ?

          @writing = false
          @closing = false
          @mutex = Mutex.new # needed?

          @log.trace "connection accepted", peeraddr: @peeraddr
        end

        def on_connect
          @connect_callback.call(self) # TODO: self? original connection object?
        end

        def data(&callback)
          @data_callback = callback
        end

        def on_read(data)
          @data_callback.call(data)
        rescue => e
          @log.error "unexpected error on reading data", peeraddr: @peeraddr, error: e
          @log.error_backtrace
          close rescue nil
        end

        def close
          # TODO: check @closing and @writing, and wait to write completion
          super
        end
      end
    end
  end
end
