require 'socket'
require 'concurrent-edge'
require 'log4r'

LOGGER = Log4r::Logger.new 'TCPWorkerPool'
LOGGER.outputters = Log4r::Outputter.stdout
Log4r::Outputter.stdout.formatter = Log4r::PatternFormatter.new :pattern => "%l %x: %m"

class Worker
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
  def socket_id
    @socket.to_s
  end
  def run_task(*args)
    LOGGER.debug "Server sending #{args} to #{socket_id}"
    Marshal.dump(args, @socket)
    res, exc = Marshal.load(@socket)
    LOGGER.debug "Server received #{res} #{exc} from #{socket_id}"
    raise exc if exc
    res
  end
end

class TCPWorkerPool
  def initialize(tcp_port = 2000)
    @tcp_port = tcp_port
    @workers = Queue.new 
    @th_server = Thread.new(TCPServer.new(@tcp_port)) do |server|
      while !@stop do
        @workers << Worker.new(server.accept)
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
    LOGGER.debug "looking for an available socket. #{@workers.size} already available"
    begin
      w=@workers.pop
    end until !w.closed?
    LOGGER.debug "found an available socket #{w.socket_id}"
    w
  end

  def release_worker(worker)
    LOGGER.debug "release worker #{worker.socket_id}"
    @workers.push(worker) unless worker.closed?
  end

  def future(*args)
    LOGGER.debug "Server asked for #{args}"
    Concurrent.future do
      w = acquire_worker
      begin
        LOGGER.debug "worker acquired: #{w.socket_id}"
        w.run_task(*args)
      ensure
        release_worker(w)
      end
    end
  end
end

class TCPClient
  def initialize(addr, port)
    @socket = TCPSocket.new(addr, port)
    LOGGER.info "Client connected to #{addr}:#{port}"
  end
  def run_task(args)
    [args.first.is_a?(Symbol) ? send(*args) : args.first.send(*args[1..-1]), nil]
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


