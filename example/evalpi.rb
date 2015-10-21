require_relative '../lib/concurrent-ruby-tcp' # rubocop:disable Style/FileName
require 'log4r'

TCP_PORT = 2_000

######################
#  SHARED FUNCTIONS  #
######################

class ExampleContext
  include Concurrent::Edge::Context # required module for distributed call
  attr_accessor :call_count
  def initialize
    @call_count = 0
  end

  # shared functions are expected to be 'pure': without side effects
  # there is no synchronisation mechanism for global/shared variables
  def average(*values)
    v = values.compact # remove nil values due to random failures
    v.reduce(:+) / v.size
  end

  def eval_pi(trial_count)
    @call_count += 1 # local state (not shared between executors)
    r = Random.new
    # Random failure
    fail 'eval_pi random failure' if r.rand < 0.1
    # computation
    (4.0 / trial_count) * trial_count.times.count do
      r.rand**2 + r.rand**2 < 1
    end
  end
end

#####################
#  CLIENT SPECIFIC  #
#####################

def client_main(id)
  Log4r::NDC.push("client-#{id}")
  rcaller = ExampleContext.new
  Concurrent::Edge::TCPClient.new('localhost', TCP_PORT, rcaller).tap(&:run)
  LOGGER.info "exiting after #{rcaller.call_count} call to eval_pi"
rescue => e
  LOGGER.fatal "client exception #{e}\n#{e.backtrace.join("\n")}"
ensure
  Log4r::NDC.pop
end

#####################
#  SERVER SPECIFIC  #
#####################

def distributed_eval_pi(server)
  # evaluate pi with the average results of several remote to eval_pi
  task_count = 50
  trials_per_task = 1_000_000
  futures = task_count.times.map do
    # Concurrent.future should be given a block build using `server.proc` so that it can be remotely distributed
    Concurrent.future(&server.proc(:eval_pi, trials_per_task))
    .rescue do |e|
      fail e unless e.is_a?(RuntimeError)
      LOGGER.debug "rescue expected error: #{e}"
      nil
    end
  end
  # usual Concurrent.future methods are available: for instance `#rescue` or `#then`
  Concurrent.zip(*futures).then(&server.proc(:average))
end

def start_clients(client_count)
  clients = client_count.times.map do |id|
    LOGGER.info "starting client #{id}"
    pid = Kernel.spawn(ENV, RbConfig.ruby, __FILE__, 'client', id.to_s, out: :out, err: :err)
    sleep 1 # start slowly to check that futures are correctly load balanced
    pid
  end
  yield clients
  clients.each { |pid| Process.wait pid }
end

def server_main
  Log4r::NDC.push('server')
  rcaller = ExampleContext.new
  # use two local workers in addition to TCPWorkers
  # the total number of parallel worker and the job queue rely on concurrent-ruby executor
  local_workers = 2.times.map { |i| Concurrent::Edge::LocalWorker.new(rcaller, i.to_s) }
  server = Concurrent::Edge::TCPWorkerPool.new(local_workers).tap { |s| s.listen(TCP_PORT) }

  LOGGER.info 'Create futures'
  pi = distributed_eval_pi(server)
  LOGGER.info "Futures created. #{rcaller.call_count} futures already executed."

  start_clients(4) do
    LOGGER.info 'Wait for future'
    puts "pi evaluated to #{pi.value!}"
    server.stop!
  end

  LOGGER.info "exiting after #{rcaller.call_count} call to eval_pi"
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
