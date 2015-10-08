require 'socket'
require 'concurrent-edge'
require 'log4r'

LOGGER = Log4r::Logger.new 'TCPWorkerPool'
LOGGER.outputters = Log4r::Outputter.stdout
Log4r::Outputter.stdout.formatter = Log4r::PatternFormatter.new :pattern => "%l %x: %m"

module Concurrent
  module Edge

    module Remote
      def call(*args)
        args.first.is_a?(Symbol) ? send(*args) : args.first.send(*args[1..-1])
      end
      def future(executor=:io, *args)
        case (executor)
        when TCPWorkerPool
          executor.future(*args)
        else
          Concurrent.future(executor) { call(*args) }
        end
      end
    end

    class TCPWorker
      def initialize(socket)
        LOGGER.debug "Server received new connection #{socket}"
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
        LOGGER.debug "Server sending #{args} to #{name}"
        Marshal.dump(args, @socket)
        res, exc = Marshal.load(@socket)
        LOGGER.debug "Server received #{res} #{exc} from #{name}"
        raise exc if exc
        res
      end
    end

    class LocalWorker
      attr_reader :name
      def initialize(eval_context, name = self.to_s)
        @name = name
        @context = eval_context
        LOGGER.debug "create local worker #{name}"
      end
      def closed?
        false
      end
      def close
      end
      def run_task(*args)
        LOGGER.debug "Server sending #{args} to local worker #{@name}"
        @context.call(*args)
      end
    end

    class TCPWorkerPool
      def initialize(tcp_port, default_executor = :io, extra_workers = [])
        @tcp_port = tcp_port
        @default_executor = default_executor
        @workers = Queue.new 
        extra_workers.each{ |w| @workers << w }
        @th_server = Thread.new(TCPServer.new(@tcp_port)) do |server|
          while !@stop do
            @workers << TCPWorker.new(server.accept)
          end
          close_workers
          server.close
        end
        LOGGER.info "Server started on port #{@tcp_port}"
      end

      def close_workers
        loop do
          @workers.pop(true).close
        end
      rescue ThreadError # empty queue
      end

      def stop!
        @stop = true
        # make sure that server is not stucked in @server.accept
        TCPSocket.new('localhost', @tcp_port).close
        @th_server.join
      end

      def acquire_worker
        LOGGER.debug "looking for an available worker. #{@workers.size} already available"
        begin
          w=@workers.pop
        end until !w.closed?
        LOGGER.debug "found an available worker #{w.name}"
        w
      end

      def release_worker(worker)
        LOGGER.debug "release worker #{worker.name}"
        @workers.push(worker) unless worker.closed?
      end

      def future(*args)
        LOGGER.debug "Server asked for #{args}"
        Concurrent.future(@default_executor) do
          w = acquire_worker
          begin
            LOGGER.debug "worker acquired: #{w.name}"
            w.run_task(*args)
          ensure
            release_worker(w)
          end
        end
      end
    end

    class TCPClient
      def initialize(addr, port, eval_context)
        @socket = TCPSocket.new(addr, port)
        @context = eval_context
        LOGGER.info "Client connected to #{addr}:#{port}"
      end
      def run_task(args)
        [@context.call(*args), nil]
      rescue => e
        [nil, e]
      end
      def run
        while(!@socket.closed?) do
          args = Marshal.load(@socket)
          LOGGER.debug "Client received #{args}"
          res = run_task(args)
          LOGGER.debug "Client send results #{res}"
          Marshal.dump(res, @socket)
        end
      rescue EOFError => e
        # normal error: socket closed from remote head while Marshal.load is waiting for data
        LOGGER.debug "Client disconnected by server"
      rescue => e
        LOGGER.error "#{e}\n#{e.backtrace.join("\n")}"
      end
    end

  end
end

