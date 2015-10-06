require 'socket'
require 'concurrent-edge'
require 'log4r'

LOGGER = Log4r::Logger.new 'TCPWorkerPool'
LOGGER.outputters = Log4r::Outputter.stdout
Log4r::Outputter.stdout.formatter = Log4r::PatternFormatter.new :pattern => "%l %x: %m"

class Worker
  attr_accessor :ready, :socket
  def initialize(socket)
    LOGGER.debug "Server received new connection #{socket}"
    @socket = socket
    @ready = true
  end
  def close
    @socket.close
  end
  def run_task(*args)
    LOGGER.debug "Server sending #{args} to #{socket}"
    Marshal.dump(args, @socket)
    res, exc = Marshal.load(@socket)
    LOGGER.debug "Server received #{res} #{exc} from #{socket}"
    raise exc if exc
    res
  end
end

class TCPWorkerPool
  def initialize(tcp_port = 2000)
    @tcp_port = tcp_port
    @server = TCPServer.new @tcp_port
    LOGGER.info "Server started on port #{@tcp_port}"
    @workers = [] 
    @worker_waiting_list = []
    @th_server = Thread.new do
      while !@stop do
        @workers << (worker = Worker.new(@server.accept))
        signal_available_worker(worker)
      end
      @workers.map(&:close)
      @server.close
    end
  end

  def stop!
    @stop = true
    # make sure that server is not stucked in @server.accept
    TCPSocket.new('localhost', @tcp_port).close
    @th_server.join
  end

  def signal_available_worker(worker)
    worker.ready = true
    future = @worker_waiting_list.pop
    if future
      LOGGER.debug "Server has found an available worker #{worker.socket}"
      worker.ready = false
      future.success(worker) 
    end
  end

  def wait_for_worker
    LOGGER.debug "Server waiting for available worker"
    @worker_waiting_list << Concurrent.future
    @worker_waiting_list.last
  end

  def exec_on_worker(worker, args)
    worker.run_task(*args)
  ensure
    signal_available_worker(worker)
  end

  def future(*args)
    LOGGER.debug "Server asked for #{args}"
    ready_worker = @workers.first(&:ready)
    if ready_worker
      ready_worker.ready = false
      Concurrent.future { exec_on_worker(ready_worker, args) }
    else
      wait_for_worker.then { |w| exec_on_worker(w, args) }
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


