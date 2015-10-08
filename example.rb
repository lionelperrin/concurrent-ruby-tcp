require_relative 'lib/concurrent-ruby-tcp'

TCP_PORT = 2_000

# shared functions
module RemoteFunctions
  def eval_pi(trial_count)
    @call_count += 1
    r = Random.new
    #Random failure
    raise RuntimeError, "eval_pi random failure" if r.rand < 0.1
    # computation
    (4.0/trial_count) * trial_count.times.count do
      r.rand**2+r.rand**2 < 1
    end 
  end
end

# client class
class RemoteCaller
  attr_accessor :call_count
  def initialize
    @call_count = 0
  end
  include Concurrent::Edge::Remote
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
  Kernel.spawn(ENV, RbConfig.ruby, __FILE__, 'client', id.to_s, :out => :out, :err => :err)
end

def server_main
  Log4r::NDC.push('server')
  rcaller = RemoteCaller.new
  # use two local threads in addition to TCPWorkers
  local_workers = 2.times.map{ |i| Concurrent::Edge::LocalWorker.new(rcaller, i.to_s) }
  server = Concurrent::Edge::TCPWorkerPool.new(TCP_PORT, local_workers)

  # eval_pi using several remote call to eval_pi
  task_count = 50
  trials_per_task = 1_000_000
  futures = task_count.times.map do
    rcaller.future(server, :eval_pi, trials_per_task)
      .rescue{ |e| raise e unless e.is_a?(RuntimeError); LOGGER.debug "rescue expected error: #{e}"; nil }
  end
  pi = Concurrent.zip(*futures).then do |*values|
    v = values.compact # remove nil values due to random failures
    v.reduce(:+) / v.size
  end

  # start slowly slaves to check that tasks are correctly load balanced
  children = 4.times.map {|i| start_client(i).tap{ sleep 1 } }
  LOGGER.debug "children: #{children}"

  puts "pi evaluated to #{pi.value!}"

  server.stop!
  children.each { |pid| Process.wait pid }
  LOGGER.info "exiting after #{rcaller.call_count} call to eval_pi"
ensure
  Log4r::NDC.pop
end

if $0 == __FILE__
  # LOGGER.level = Log4r::INFO
  if ARGV[0] == 'client'
    client_main(ARGV[1])
  else
    server_main
  end
end



