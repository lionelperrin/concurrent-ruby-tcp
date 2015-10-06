require 'socket'
require 'concurrent-edge'
require 'log4r'

LOGGER = Log4r::Logger.new 'TCPWorkerPool'
LOGGER.outputters = Log4r::Outputter.stdout

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
    Marshal.load(@socket)
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
    end
  end

  def stop!
    @stop = true
    # make sure that server is not stucked in @server.accept
    TCPSocket.new('localhost', @tcp_port).close
    @workers.map(&:close)
    @server.close
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

  def future(*args)
    LOGGER.debug "Server asked for #{args}"
    ready_worker = @workers.first(&:ready)
    if ready_worker
      ready_worker.ready = false
      Concurrent.future { ready_worker.run_task(*args).tap{ signal_available_worker(ready_worker) } }
    else
      wait_for_worker.then{ |w| w.run_task(*args).tap{ signal_available_worker(w) } }
    end
  end
end

class TCPClient
  def initialize(addr, port)
    @socket = TCPSocket.new(addr, port)
    LOGGER.info "Client connected to #{addr}:#{port}"
  end
  def run
    while(!@socket.closed?) do
      args = Marshal.load(@socket)
      LOGGER.debug "Client received #{args}"
      res = args.first.is_a?(Symbol) ? send(*args) : args.first.send(*args[1..-1])
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

module RemoteFunctions
  def add(*args)
    args.reduce(:+)
  end
end

class MyTCPClient < TCPClient
  include RemoteFunctions
end

server = TCPWorkerPool.new(2000)
threads = 10.times.map do |i|
  Thread.new do
    begin
      MyTCPClient.new('localhost', 2000).run
    rescue => e
      LOGGER.fatal "Thread exception #{e}\n#{e.backtrace.join("\n")}"
    end
  end
end

f1 = server.future(1, :+, 2)
f2 = server.future(:add, 3, 4)
(f1 & f2).then{|v1, v2| server.future(v1, :+, v2).value}.then{|v| puts "!!! final result: #{v} !!!"}.value!

server.stop!
threads.map(&:join)

