require 'socket'
require 'concurrent-edge'
require 'log4r'

module Concurrent
  module Edge
    TCP_LOGGER = Log4r::Logger.new 'concurrent-ruby-tcp'

    module Context
      def call(*args)
        args.map(&:freeze) # do not allow parameters to be modified
        args.first.is_a?(Symbol) ? send(*args) : args.first.send(*args[1..-1])
      end
    end

    class TCPWorker
      def initialize(socket)
        TCP_LOGGER.debug "Server received new connection #{socket}"
        @socket = socket
      end

      def closed?
        @socket.closed?
      end

      def close
        @socket.close
      end

      def name
        @socket.to_s
      end

      def run_task(*args)
        TCP_LOGGER.debug "Server sending #{args} to #{name}"
        Marshal.dump(args, @socket)
        res, exc = Marshal.load(@socket)
        TCP_LOGGER.debug "Server received #{res} #{exc} from #{name}"
        fail exc if exc
        res
      end
    end

    class LocalWorker
      attr_reader :name
      def initialize(eval_context, name = to_s)
        @name = name
        @context = eval_context
        TCP_LOGGER.debug "create local worker #{name}"
      end

      def closed?
        false
      end

      def close
      end

      def run_task(*args)
        TCP_LOGGER.debug "Server sending #{args} to local worker #{@name}"
        @context.call(*args)
      end
    end

    class TCPWorkerPool
      SELECT_TIMEOUT = 1
      def initialize(extra_workers = [])
        @workers = Queue.new
        extra_workers.each { |w| @workers << w }
      end

      def listen(tcp_port)
        fail "already listening on #{@tcp_port}" if @tcp_port
        @tcp_port = tcp_port
        @tcp_server = TCPServer.new(@tcp_port)
        @th_server = Thread.new(@tcp_server) do |server|
          begin
            loop do
              @workers << TCPWorker.new(tcp_accept(server))
            end
          rescue Errno::EBADF, Errno::ENOTSOCK, IOError => e
            # server closed
            TCP_LOGGER.debug("server.accept received #{e}")
          ensure
            close_workers
          end
        end
        TCP_LOGGER.info "Server started on port #{@tcp_port}"
      end

      def stop!
        @tcp_server.close if @tcp_server
        @th_server.join if @th_server
      end

      def proc(*fixed_args)
        Proc.new do |*args|
          _args = fixed_args + args # similar to curry
          TCP_LOGGER.debug "Server asked for #{_args}"
          w = acquire_worker
          begin
            TCP_LOGGER.debug "worker acquired: #{w.name}"
            w.run_task(*_args)
          rescue EOFError, Errno::ECONNABORTED => e
            TCP_LOGGER.debug "client disconnected: #{e}"
            w.close
            raise
          ensure
            release_worker(w)
          end
        end
      end

      private

      def tcp_accept(serv)
        begin # emulate blocking accept
          sock = serv.accept_nonblock
        rescue IO::WaitReadable, Errno::EINTR
          # wake up after 1 second or if a connection is available
          IO.select([serv], [], [], SELECT_TIMEOUT)
          retry
        end
        sock
      end

      def close_workers
        loop do
          @workers.pop(true).close
        end
      rescue ThreadError # empty queue
      end

      def acquire_worker
        TCP_LOGGER.debug "looking for an available worker. #{@workers.size} already available"
        w = nil
        loop do
          w = @workers.pop
          break unless w.closed?
        end
        TCP_LOGGER.debug "found an available worker #{w.name}"
        w
      end

      def release_worker(worker)
        TCP_LOGGER.debug "release worker #{worker.name}"
        @workers.push(worker) unless worker.closed?
      end
    end

    class TCPClient
      def initialize(addr, port, eval_context)
        @socket = TCPSocket.new(addr, port)
        @context = eval_context
        TCP_LOGGER.info "Client connected to #{addr}:#{port}"
      end

      def run_task(args)
        [@context.call(*args), nil]
      rescue => e
        [nil, e]
      end

      def run
        until @socket.closed?
          args = Marshal.load(@socket)
          TCP_LOGGER.debug "Client received #{args}"
          res = run_task(args)
          TCP_LOGGER.debug "Client send results #{res}"
          Marshal.dump(res, @socket)
        end
      rescue Errno::ECONNRESET, Errno::ECONNABORTED, EOFError => e
        # normal error: socket closed from Context head while Marshal.load is waiting for data
        TCP_LOGGER.debug "Client disconnected by server #{e}"
      rescue => e
        TCP_LOGGER.error "#{e}\n#{e.backtrace.join("\n")}"
      ensure
        @socket.close
      end
    end
  end
end
