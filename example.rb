require_relative 'lib/concurrent-ruby-tcp'
require 'log4r'

TCP_PORT = 2_000

# shared functions
module RemoteFunctions
  def eval_pi(trial_count)
    @call_count += 1
    r = Random.new
    # Random failure
    fail 'eval_pi random failure' if r.rand < 0.1
    # computation
    (4.0 / trial_count) * trial_count.times.count do
      r.rand**2 + r.rand**2 < 1
    end
  end
end

# client class
class RemoteCaller
  attr_accessor :call_count
  def initialize
    @call_count = 0
  end
  include Concurrent::Edge::Context
  include RemoteFunctions
end

def client_main(id)
  Log4r::NDC.push("client-#{id}")
  rcaller = RemoteCaller.new
  c = Concurrent::Edge::TCPClient.new('localhost', TCP_PORT, rcaller).tap(&:run)
  LOGGER.info "exiting after #{rcaller.call_count} call to eval_pi"
rescue => e
  LOGGER.fatal "client exception #{e}\n#{e.backtrace.join("\n")}"
ensure
  Log4r::NDC.pop
end

def start_client(id)
  LOGGER.info "starting client #{id}"
  Kernel.spawn(ENV, RbConfig.ruby, __FILE__, 'client', id.to_s, out: :out, err: :err)
end

def future_eval_pi(server, rcaller)
  # eval_pi using several remote call to eval_pi
  task_count = 50
  trials_per_task = 1_000_000
  futures = task_count.times.map do
    rcaller.future(server, :eval_pi, trials_per_task)
    .rescue { |e| fail e unless e.is_a?(RuntimeError); LOGGER.debug "rescue expected error: #{e}"; nil }
  end
  pi = Concurrent.zip(*futures).then do |*values|
    v = values.compact # remove nil values due to random failures
    v.reduce(:+) / v.size
  end
end

def server_main
  Log4r::NDC.push('server')
  rcaller = RemoteCaller.new
  # use two local threads in addition to TCPWorkers
  local_workers = 2.times.map { |i| Concurrent::Edge::LocalWorker.new(rcaller, i.to_s) }
  server = Concurrent::Edge::TCPWorkerPool.new(
    Concurrent::ThreadPoolExecutor.new(
      max_threads: 8, # at most 8 threads for TCPWorkerPool
      max_queue: 100, # maximum 100 tasks queued
      fallback_policy: :caller_runs # execute in caller thread when no thread is available
    ),
    local_workers).tap { |s| s.listen(TCP_PORT) }

  LOGGER.info 'Create futures'
  pi = future_eval_pi(server, rcaller)
  LOGGER.info "Futures created. #{rcaller.call_count} futures already executed."

  # start slowly slaves to check that tasks are correctly load balanced
  children = 4.times.map { |i| start_client(i).tap { sleep 1 } }
  LOGGER.debug "children: #{children}"

  LOGGER.info 'Wait for future'
  puts "pi evaluated to #{pi.value!}"

  server.stop!
  children.each { |pid| Process.wait pid }
  LOGGER.info "exiting after #{rcaller.call_count} call to eval_pi"
ensure
  Log4r::NDC.pop
end

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
