require_relative '../lib/concurrent-ruby-tcp' # rubocop:disable Style/FileName
require 'log4r'

TCP_PORT = 2_000

######################
#  SHARED FUNCTIONS  #
######################

class ExampleContext
  include Concurrent::Edge::Context # required module for distributed call

  # shared functions are expected to be 'pure': without side effects
  # there is no synchronisation mechanism for global/shared variables
  def hostname
    Socket.gethostname
  end
end

#####################
#  CLIENT SPECIFIC  #
#####################

def client_main(id)
  Log4r::NDC.push("client-#{id}")
  rcaller = ExampleContext.new
  Concurrent::Edge::TCPClient.new('localhost', TCP_PORT, rcaller).tap(&:run)
  LOGGER.info 'exiting'
rescue => e
  LOGGER.fatal "client exception #{e}\n#{e.backtrace.join("\n")}"
ensure
  Log4r::NDC.pop
end

#####################
#  SERVER SPECIFIC  #
#####################

def start_clients(client_count)
  clients = client_count.times.map do |id|
    LOGGER.info "starting client #{id}"
    pid = Kernel.spawn(ENV, RbConfig.ruby, __FILE__, 'client', id.to_s, out: :out, err: :err)
    pid
  end
  yield clients
  clients.each { |pid| Process.wait pid }
end

def server_main
  Log4r::NDC.push('server')
  rcaller = ExampleContext.new
  # the total number of parallel worker and the job queue rely on concurrent-ruby executor
  server = Concurrent::Edge::TCPWorkerPool.new.tap do |s|
    # setup workers when available
    s.setup_workers_with(:hostname)
    # use two local workers in addition to TCPWorkers
    2.times{ |i| s.add_worker Concurrent::Edge::LocalWorker.new(rcaller, i.to_s) }
    # listen on TCP port
    s.listen(TCP_PORT)
  end

  start_clients(4) do
    sleep(1)
    puts "workers initialized with #{server.workers_status}"
    server.stop!
  end
ensure
  Log4r::NDC.pop
end

#####################
#    SHARED MAIN    #
#####################

if $PROGRAM_NAME == __FILE__
  Log4r::Outputter.stdout.formatter = Log4r::PatternFormatter.new pattern: '%d %l %x: %m'
  LOGGER = Log4r::Logger.new 'example'
  LOGGER.level = Log4r::INFO
  LOGGER.outputters = Log4r::Outputter.stdout
  Log4r::Logger['concurrent-ruby-tcp'].level = Log4r::DEBUG
  Log4r::Logger['concurrent-ruby-tcp'].outputters = Log4r::Outputter.stdout
  if ARGV[0] == 'client'
    client_main(ARGV[1])
  else
    server_main
  end
end
